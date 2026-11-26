package io.github.sudoitir.openbroker.kafka.schema;

/**
 * The kind of schema a subject holds. The three are the wire types a Kafka schema registry writes a
 * schema id in front of. The framing starts the same way for all of them, a magic byte and the id. Protobuf then
 * has a count of message indexes; Avro and JSON do not.
 */
public enum SchemaType {
    AVRO,
    PROTOBUF,
    JSON
}
