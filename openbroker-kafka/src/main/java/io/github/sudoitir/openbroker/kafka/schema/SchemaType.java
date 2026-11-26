package io.github.sudoitir.openbroker.kafka.schema;

/**
 * The kind of schema a subject holds. The three are the wire types a Kafka schema registry writes a
 * schema id in front of; the framing is the same for all of them.
 */
public enum SchemaType {
    AVRO,
    PROTOBUF,
    JSON
}
