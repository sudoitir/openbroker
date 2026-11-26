package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/** The three serdes round-trip a record without a registry. Framing is what a decode reads back. */
class SchemaSerdeTest {

    @Test
    void avroProtobufAndJsonRoundTrip() {
        Map<String, Object> record = Map.of("city", "Shiraz", "count", 3L, "open", true);
        for (String name : new String[] {SerdeRegistry.AVRO, SerdeRegistry.PROTOBUF, SerdeRegistry.JSON}) {
            String schema = SerdeRegistry.schemaOf(name, record);
            byte[] framed = SerdeRegistry.frame(name, 7, schema, record);
            assertThat(framed[0]).isZero();
            DecodedRecord decoded = SerdeRegistry.decode(
                    name,
                    SerdeRegistry.typeOf(name),
                    7,
                    "orders-value",
                    schema,
                    Arrays.copyOfRange(framed, 5, framed.length));
            assertThat(decoded).isInstanceOf(DecodedRecord.Decoded.class);
            assertThat(((DecodedRecord.Decoded) decoded).record()).isEqualTo(record);
        }
    }

    @Test
    void aClassNameIsNotASerde() {
        assertThat(SerdeRegistry.known("com.example.Order")).isFalse();
    }

    @Test
    void avroBytesAndFixedBecomeByteArrays() throws Exception {
        Map<String, Object> record = Map.of("blob", new byte[] {1, 2, 3});
        String schema = SerdeRegistry.schemaOf(SerdeRegistry.AVRO, record);
        byte[] framed = SerdeRegistry.frame(SerdeRegistry.AVRO, 1, schema, record);
        DecodedRecord decoded = SerdeRegistry.decode(
                SerdeRegistry.AVRO, SchemaType.AVRO, 1, "s", schema, Arrays.copyOfRange(framed, 5, framed.length));
        assertThat((byte[]) ((Map<?, ?>) ((DecodedRecord.Decoded) decoded).record()).get("blob"))
                .isEqualTo(new byte[] {1, 2, 3});

        String fixed =
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"fixed\",\"name\":\"Id\",\"size\":2}}]}";
        Schema parsed = new Schema.Parser().parse(fixed);
        GenericRecord written = new GenericData.Record(parsed);
        written.put("id", new GenericData.Fixed(parsed.getField("id").schema(), new byte[] {9, 8}));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(parsed).write(written, encoder);
        encoder.flush();
        DecodedRecord fixedDecoded =
                SerdeRegistry.decode(SerdeRegistry.AVRO, SchemaType.AVRO, 1, "s", fixed, out.toByteArray());
        Map<String, List<Byte>> viewed = new LinkedHashMap<>();
        byte[] id = (byte[]) ((Map<?, ?>) ((DecodedRecord.Decoded) fixedDecoded).record()).get("id");
        viewed.put("id", List.of(id[0], id[1]));
        assertThat(viewed).containsEntry("id", List.of((byte) 9, (byte) 8));
    }

    @Test
    void aHugeMessageIndexCountIsRefused() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int zig = (1_000_000_000 << 1);
        while ((zig & ~0x7f) != 0) {
            out.write((zig & 0x7f) | 0x80);
            zig >>>= 7;
        }
        out.write(zig);
        DecodedRecord decoded = SerdeRegistry.decode(
                SerdeRegistry.PROTOBUF, SchemaType.PROTOBUF, 1, "s", "syntax = \"proto3\";", out.toByteArray());
        assertThat(decoded).isInstanceOf(DecodedRecord.Undecoded.class);
        assertThat(((DecodedRecord.Undecoded) decoded).reason()).isEqualTo(DecodedRecord.SERDE_REFUSED);
    }

    @Test
    void aShortPasswordIsStrippedWhenItStandsAlone() {
        assertThat(RegistryHttp.Redactor.of("u", "ab").apply("failed ab now")).doesNotContain("ab");
        assertThat(RegistryHttp.Redactor.of("u", "ab").apply("about")).contains("about");
        assertThat(RegistryHttp.Redactor.of("u", "ab").leaks("failed ab now")).isTrue();
    }
}
