package io.github.sudoitir.openbroker.kafka.schema;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The serdes an installation runs, by name. Lookup is this map and nothing else: a name is never loaded as a class.
 *
 * <p>A null field is omitted. A schema that imports another file is refused; pass the imported text when a caller has
 * one.
 */
final class SerdeRegistry {

    static final String AVRO = "avro";
    static final String PROTOBUF = "protobuf";
    static final String JSON = "json";

    private static final ObjectMapper JSON_MAPPER = JsonMapper.builder().build();
    private static final System.Logger LOG = System.getLogger(SerdeRegistry.class.getName());

    private SerdeRegistry() {}

    static boolean known(String name) {
        return AVRO.equals(name) || PROTOBUF.equals(name) || JSON.equals(name);
    }

    static SchemaType typeOf(String name) {
        return switch (name) {
            case AVRO -> SchemaType.AVRO;
            case PROTOBUF -> SchemaType.PROTOBUF;
            case JSON -> SchemaType.JSON;
            default -> throw new IllegalArgumentException("No serde named " + name + " is registered.");
        };
    }

    /** The schema text a record of this shape registers under, stable for the same fields. */
    static String schemaOf(String name, Map<String, ?> record) {
        Map<String, ?> fields = present(record);
        return switch (name) {
            case AVRO -> avroSchema(fields);
            case PROTOBUF -> protoSchema(fields);
            case JSON -> jsonSchema(fields);
            default -> throw new IllegalArgumentException("No serde named " + name + " is registered.");
        };
    }

    /** Magic byte, schema id, and the payload a {@link #decode} of this serde reads back. */
    static byte[] frame(String name, int schemaId, String schema, Map<String, ?> record) {
        byte[] payload =
                switch (name) {
                    case AVRO -> avroWrite(schema, record);
                    case PROTOBUF -> protobufWrite(schema, record);
                    case JSON -> jsonWrite(record);
                    default -> throw new IllegalArgumentException("No serde named " + name + " is registered.");
                };
        ByteBuffer framed = ByteBuffer.allocate(5 + payload.length);
        framed.put((byte) 0);
        framed.putInt(schemaId);
        framed.put(payload);
        return framed.array();
    }

    static DecodedRecord decode(
            String serdeName, SchemaType type, int schemaId, String subject, String schema, byte[] payload) {
        String name = serdeName != null ? serdeName : nameOf(type);
        if (!known(name)) {
            return new DecodedRecord.Undecoded(DecodedRecord.SERDE_NOT_REGISTERED);
        }
        try {
            Object record =
                    switch (name) {
                        case AVRO -> avroRead(schema, payload);
                        case PROTOBUF -> protobufRead(schema, payload);
                        case JSON -> jsonRead(payload);
                        default -> throw new IllegalArgumentException(name);
                    };
            return new DecodedRecord.Decoded(type, schemaId, subject, record);
        } catch (Truncated e) {
            return new DecodedRecord.Undecoded(DecodedRecord.TRUNCATED);
        } catch (RuntimeException e) {
            LOG.log(
                    System.Logger.Level.DEBUG,
                    "A serde refused a payload: " + e.getClass().getName());
            return new DecodedRecord.Undecoded(DecodedRecord.SERDE_REFUSED);
        }
    }

    private static String nameOf(SchemaType type) {
        return switch (type) {
            case AVRO -> AVRO;
            case PROTOBUF -> PROTOBUF;
            case JSON -> JSON;
        };
    }

    private static Map<String, ?> present(Map<String, ?> record) {
        Map<String, Object> out = new TreeMap<>();
        record.forEach((key, value) -> {
            if (value != null) {
                out.put(key, value);
            }
        });
        return out;
    }

    private static void checkName(String name) {
        if (name.isEmpty() || !Character.isLetter(name.charAt(0)) && name.charAt(0) != '_') {
            throw new IllegalArgumentException("A field name must start with a letter: " + name);
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                throw new IllegalArgumentException("A field name must be a schema name: " + name);
            }
        }
    }

    private static String avroSchema(Map<String, ?> record) {
        StringBuilder fields = new StringBuilder();
        for (Map.Entry<String, ?> field : record.entrySet()) {
            checkName(field.getKey());
            if (!fields.isEmpty()) {
                fields.append(',');
            }
            fields.append("{\"name\":")
                    .append(quote(field.getKey()))
                    .append(",\"type\":")
                    .append(avroType(field.getKey(), field.getValue()))
                    .append('}');
        }
        return "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[" + fields + "]}";
    }

    private static String avroType(String name, Object value) {
        if (value instanceof String) {
            return "\"string\"";
        }
        if (value instanceof Boolean) {
            return "\"boolean\"";
        }
        if (value instanceof Number number) {
            return integral(number) ? "\"long\"" : "\"double\"";
        }
        if (value instanceof List<?> list) {
            return "{\"type\":\"array\",\"items\":" + avroType(name, element(list)) + "}";
        }
        if (value instanceof Map<?, ?> map) {
            return avroSchema(present(cast(map))).replace("\"name\":\"Record\"", "\"name\":" + quote(nested(name)));
        }
        throw new IllegalArgumentException("A field has no schema type: " + name);
    }

    private static byte[] avroWrite(String schemaText, Map<String, ?> record) {
        Schema schema = new Schema.Parser().parse(schemaText);
        GenericRecord written = avroRecord(schema, record);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(written, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static GenericRecord avroRecord(Schema schema, Map<String, ?> record) {
        GenericRecord written = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.name());
            if (value != null) {
                written.put(field.name(), avroValue(field.schema(), value));
            }
        }
        return written;
    }

    private static Object avroValue(Schema schema, Object value) {
        return switch (schema.getType()) {
            case LONG -> ((Number) value).longValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case ARRAY ->
                ((List<?>) value)
                        .stream()
                                .map(item -> avroValue(schema.getElementType(), item))
                                .toList();
            case RECORD -> avroRecord(schema, cast(value));
            default -> value;
        };
    }

    private static Object avroRead(String schemaText, byte[] payload) {
        Schema schema = new Schema.Parser().parse(schemaText);
        try {
            GenericRecord read = new GenericDatumReader<GenericRecord>(schema)
                    .read(null, DecoderFactory.get().binaryDecoder(payload, null));
            return fromAvro(read);
        } catch (IOException e) {
            throw new IllegalArgumentException("Avro refused the payload", e);
        }
    }

    private static Object fromAvro(Object value) {
        if (value instanceof Utf8 utf8) {
            return utf8.toString();
        }
        if (value instanceof GenericRecord record) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                Object fieldValue = record.get(field.name());
                if (fieldValue != null) {
                    out.put(field.name(), fromAvro(fieldValue));
                }
            }
            return out;
        }
        if (value instanceof Collection<?> list) {
            return list.stream().map(SerdeRegistry::fromAvro).toList();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((key, item) -> out.put(String.valueOf(fromAvro(key)), fromAvro(item)));
            return out;
        }
        return value;
    }

    private static String protoSchema(Map<String, ?> record) {
        StringBuilder nested = new StringBuilder();
        StringBuilder fields = new StringBuilder();
        int number = 1;
        for (Map.Entry<String, ?> field : record.entrySet()) {
            checkName(field.getKey());
            fields.append("  ")
                    .append(protoField(field.getKey(), field.getValue(), nested))
                    .append(' ')
                    .append(field.getKey())
                    .append(" = ")
                    .append(number++)
                    .append(";\n");
        }
        return "syntax = \"proto3\";\nmessage Record {\n" + nested + fields + "}\n";
    }

    private static String protoField(String name, Object value, StringBuilder nested) {
        if (value instanceof String) {
            return "optional string";
        }
        if (value instanceof Boolean) {
            return "optional bool";
        }
        if (value instanceof Number number) {
            return integral(number) ? "optional int64" : "optional double";
        }
        if (value instanceof List<?> list) {
            return "repeated " + protoScalar(name, element(list), nested);
        }
        if (value instanceof Map<?, ?> map) {
            String type = nested(name);
            nested.append("  message ")
                    .append(type)
                    .append(" {\n")
                    .append(protoBody(cast(map)))
                    .append("  }\n");
            return "optional " + type;
        }
        throw new IllegalArgumentException("A field has no schema type: " + name);
    }

    private static String protoScalar(String name, Object value, StringBuilder nested) {
        if (value instanceof Map<?, ?>) {
            return protoField(name, value, nested).replace("optional ", "");
        }
        String field = protoField(name, value, nested);
        return field.startsWith("optional ") ? field.substring("optional ".length()) : field;
    }

    private static String protoBody(Map<String, ?> record) {
        StringBuilder fields = new StringBuilder();
        int fieldNumber = 1;
        for (Map.Entry<String, ?> field : record.entrySet()) {
            checkName(field.getKey());
            String type =
                    switch (field.getValue()) {
                        case String ignored -> "optional string";
                        case Boolean ignored -> "optional bool";
                        case Number value -> integral(value) ? "optional int64" : "optional double";
                        default ->
                            throw new IllegalArgumentException("A nested field must be a scalar: " + field.getKey());
                    };
            fields.append("    ")
                    .append(type)
                    .append(' ')
                    .append(field.getKey())
                    .append(" = ")
                    .append(fieldNumber++)
                    .append(";\n");
        }
        return fields.toString();
    }

    private static byte[] protobufWrite(String schema, Map<String, ?> record) {
        Descriptors.Descriptor descriptor = descriptor(schema, List.of(0));
        DynamicMessage message = protoMessage(descriptor, record);
        byte[] indexes = indexes(List.of(0));
        byte[] body = message.toByteArray();
        byte[] out = new byte[indexes.length + body.length];
        System.arraycopy(indexes, 0, out, 0, indexes.length);
        System.arraycopy(body, 0, out, indexes.length, body.length);
        return out;
    }

    private static Object protobufRead(String schema, byte[] payload) {
        IndexList indexes = readIndexes(payload);
        Descriptors.Descriptor descriptor = descriptor(schema, indexes.values());
        try {
            byte[] body = new byte[payload.length - indexes.offset()];
            System.arraycopy(payload, indexes.offset(), body, 0, body.length);
            return fromProto(DynamicMessage.parseFrom(descriptor, body));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Protobuf refused the payload", e);
        }
    }

    private static DynamicMessage protoMessage(Descriptors.Descriptor descriptor, Map<String, ?> record) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            Object value = record.get(field.getName());
            if (value == null) {
                continue;
            }
            if (field.isRepeated()) {
                for (Object item : (List<?>) value) {
                    builder.addRepeatedField(field, protoValue(field, item));
                }
            } else {
                builder.setField(field, protoValue(field, value));
            }
        }
        return builder.build();
    }

    private static Object protoValue(Descriptors.FieldDescriptor field, Object value) {
        return switch (field.getJavaType()) {
            case LONG -> ((Number) value).longValue();
            case INT -> ((Number) value).intValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case FLOAT -> ((Number) value).floatValue();
            case BOOLEAN -> value;
            case STRING -> String.valueOf(value);
            case BYTE_STRING -> ByteString.copyFrom((byte[]) value);
            case ENUM -> field.getEnumType().findValueByName(String.valueOf(value));
            case MESSAGE -> protoMessage(field.getMessageType(), cast(value));
        };
    }

    private static Object fromProto(Object value) {
        if (value instanceof DynamicMessage message) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Descriptors.FieldDescriptor field :
                    message.getDescriptorForType().getFields()) {
                if (field.isRepeated()) {
                    int count = message.getRepeatedFieldCount(field);
                    if (count == 0) {
                        continue;
                    }
                    List<Object> items = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        items.add(fromProto(message.getRepeatedField(field, i)));
                    }
                    out.put(field.getName(), items);
                } else if (message.hasField(field)) {
                    out.put(field.getName(), fromProto(message.getField(field)));
                }
            }
            return out;
        }
        if (value instanceof Descriptors.EnumValueDescriptor descriptor) {
            return descriptor.getName();
        }
        if (value instanceof ByteString bytes) {
            return bytes.toByteArray();
        }
        return value;
    }

    private static Descriptors.Descriptor descriptor(String schema, List<Integer> indexes) {
        Descriptors.FileDescriptor file;
        try {
            file = FileDescriptorUtils.protoFileToFileDescriptor(schema, "record.proto", Optional.empty());
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("The protobuf schema was refused", e);
        }
        if (indexes.isEmpty()
                || indexes.get(0) < 0
                || indexes.get(0) >= file.getMessageTypes().size()) {
            throw new IllegalArgumentException("The payload names no message in the schema.");
        }
        Descriptors.Descriptor message = file.getMessageTypes().get(indexes.get(0));
        for (int i = 1; i < indexes.size(); i++) {
            if (indexes.get(i) < 0 || indexes.get(i) >= message.getNestedTypes().size()) {
                throw new IllegalArgumentException("The payload names no message in the schema.");
            }
            message = message.getNestedTypes().get(indexes.get(i));
        }
        return message;
    }

    /** A leading 0 is the Confluent shorthand for message index 0. Otherwise a zigzag count, then that many indexes. */
    static byte[] indexes(List<Integer> indexes) {
        if (indexes.size() == 1 && indexes.get(0) == 0) {
            return new byte[] {0};
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeZigzag(indexes.size(), out);
        indexes.forEach(index -> writeZigzag(index, out));
        return out.toByteArray();
    }

    private static IndexList readIndexes(byte[] payload) {
        if (payload.length == 0) {
            throw new Truncated();
        }
        IntReader reader = new IntReader(payload);
        int count = reader.zigzag();
        if (count == 0) {
            return new IndexList(List.of(0), reader.offset());
        }
        if (count < 0) {
            throw new IllegalArgumentException("A message index count is negative.");
        }
        List<Integer> indexes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            indexes.add(reader.zigzag());
        }
        return new IndexList(indexes, reader.offset());
    }

    private static void writeZigzag(int value, ByteArrayOutputStream out) {
        int zig = (value << 1) ^ (value >> 31);
        while ((zig & ~0x7f) != 0) {
            out.write((zig & 0x7f) | 0x80);
            zig >>>= 7;
        }
        out.write(zig);
    }

    private static String jsonSchema(Map<String, ?> record) {
        StringBuilder properties = new StringBuilder();
        for (Map.Entry<String, ?> field : record.entrySet()) {
            if (!properties.isEmpty()) {
                properties.append(',');
            }
            properties.append(quote(field.getKey())).append(':').append(jsonType(field.getValue()));
        }
        return "{\"type\":\"object\",\"properties\":{" + properties + "}}";
    }

    private static String jsonType(Object value) {
        if (value instanceof String) {
            return "{\"type\":\"string\"}";
        }
        if (value instanceof Boolean) {
            return "{\"type\":\"boolean\"}";
        }
        if (value instanceof Number number) {
            return integral(number) ? "{\"type\":\"integer\"}" : "{\"type\":\"number\"}";
        }
        if (value instanceof List<?> list) {
            return "{\"type\":\"array\",\"items\":" + jsonType(element(list)) + "}";
        }
        if (value instanceof Map<?, ?> map) {
            return jsonSchema(present(cast(map)));
        }
        throw new IllegalArgumentException("A field has no schema type.");
    }

    private static byte[] jsonWrite(Map<String, ?> record) {
        return JSON_MAPPER.writeValueAsBytes(normalize(record));
    }

    private static Object jsonRead(byte[] payload) {
        try {
            return normalize(JSON_MAPPER.readValue(payload, Object.class));
        } catch (JacksonException e) {
            throw new IllegalArgumentException("JSON refused the payload", e);
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof Number number && integral(number)) {
            return number.longValue();
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((key, item) -> out.put(String.valueOf(key), normalize(item)));
            return out;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(SerdeRegistry::normalize).toList();
        }
        return value;
    }

    private static boolean integral(Number number) {
        return number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long;
    }

    private static Object element(List<?> list) {
        for (Object item : list) {
            if (item != null) {
                return item;
            }
        }
        throw new IllegalArgumentException("An empty list has no element type.");
    }

    private static String nested(String name) {
        return "Nested_" + name;
    }

    private static String quote(String text) {
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> cast(Object value) {
        return (Map<String, ?>) value;
    }

    private static final class Truncated extends RuntimeException {}

    private record IndexList(List<Integer> values, int offset) {}

    private static final class IntReader {

        private final byte[] bytes;
        private int offset;

        private IntReader(byte[] bytes) {
            this.bytes = bytes;
        }

        private int zigzag() {
            int raw = varint();
            return (raw >>> 1) ^ -(raw & 1);
        }

        private int varint() {
            int value = 0;
            int shift = 0;
            while (true) {
                if (offset >= bytes.length) {
                    throw new Truncated();
                }
                int b = bytes[offset++] & 0xff;
                value |= (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 28) {
                    throw new IllegalArgumentException("A varint does not fit an int.");
                }
            }
        }

        private int offset() {
            return offset;
        }
    }
}
