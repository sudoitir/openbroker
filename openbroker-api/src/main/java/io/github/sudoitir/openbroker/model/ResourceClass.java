package io.github.sudoitir.openbroker.model;

/**
 * The closed, neutral class every kind maps to. Shared code reasons about classes; kinds carry the
 * native names. The set is complete for the systems OpenBroker targets, so an exhaustive {@code
 * switch} over it stays valid.
 */
public enum ResourceClass {
    /** A container of other resources: a RabbitMQ virtual host, a Pulsar tenant or namespace. */
    NAMESPACE,
    /** Where messages are sent: an Artemis address, a Kafka topic, a RabbitMQ exchange, an SNS topic. */
    DESTINATION,
    /** One ordered shard of a destination: a Kafka or Pulsar partition, a super-stream partition. */
    PARTITION,
    /**
     * A backlog consumers read from, which can be browsed, measured, purged and seeked: an Artemis,
     * RabbitMQ or SQS queue, a Kafka group on one topic, a Pub/Sub subscription, a JetStream consumer.
     * Its destination relation is optional; a standalone queue has none.
     */
    SUBSCRIPTION,
    /** A set of consumers sharing work and positions: a Kafka consumer group. */
    CONSUMER_GROUP,
    /** An edge that moves messages: a binding, a divert, a bridge, a shovel, a federation link. */
    ROUTE,
    /** Settings matched to resources by pattern: Artemis address settings, RabbitMQ policies. */
    POLICY,
    /** Who may do what, by pattern: Artemis security settings, RabbitMQ permissions, Kafka ACLs. */
    ACCESS_RULE,
    /** An account or role. */
    PRINCIPAL,
    /** A client connection. */
    CONNECTION,
    /** A lane multiplexed inside a connection: an AMQP channel, a JMS or Artemis session. */
    CHANNEL,
    /** A client sending messages. */
    PRODUCER,
    /** A client receiving messages. */
    CONSUMER,
    /** A transaction the system tracks: an in-doubt XA transaction, a Kafka transactional id. */
    TRANSACTION
}
