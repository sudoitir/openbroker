package io.github.sudoitir.openbroker.kafka.schema;

import java.util.Map;

/**
 * The serdes an installation runs, by name. A name is never a class name: lookup is this map and nothing else.
 *
 * <p>The three decoders land with the framing. Until then a framed payload is refused rather than decoded wrongly.
 */
final class SerdeRegistry {

    static final String AVRO = "avro";
    static final String PROTOBUF = "protobuf";
    static final String JSON = "json";

    private SerdeRegistry() {}

    static boolean known(String name) {
        return AVRO.equals(name) || PROTOBUF.equals(name) || JSON.equals(name);
    }

    static DecodedRecord decode(
            String serdeName, SchemaType type, int schemaId, String subject, String schema, byte[] payload) {
        return new DecodedRecord.Undecoded(DecodedRecord.SERDE_REFUSED);
    }

    static byte[] encode(String serdeName, int schemaId, Map<String, ?> record, String schema) {
        throw new UnsupportedOperationException("encoding");
    }
}
