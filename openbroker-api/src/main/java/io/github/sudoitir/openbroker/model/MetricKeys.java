package io.github.sudoitir.openbroker.model;

/**
 * The standard metric attribute keys, in OpenTelemetry style. OpenTelemetry defines no backlog
 * metric, so the keys sit under the SDK's own prefix. Units are UCUM.
 */
public final class MetricKeys {

    /** Messages waiting to be delivered. UpDownCounter, {@code {message}}. */
    public static final AttributeKey<MetricValue> BACKLOG = key("openbroker.backlog");
    /** Bytes waiting to be delivered. UpDownCounter, {@code By}. */
    public static final AttributeKey<MetricValue> BACKLOG_SIZE = key("openbroker.backlog.size");
    /** Age of the oldest waiting message. Gauge, {@code s}. */
    public static final AttributeKey<MetricValue> BACKLOG_AGE = key("openbroker.backlog.age");
    /** Messages delivered and not yet settled. UpDownCounter, {@code {message}}. */
    public static final AttributeKey<MetricValue> IN_FLIGHT = key("openbroker.in_flight");
    /** Messages held for delivery later. UpDownCounter, {@code {message}}. */
    public static final AttributeKey<MetricValue> SCHEDULED = key("openbroker.scheduled");
    /** Messages ever added. Counter, {@code {message}}. */
    public static final AttributeKey<MetricValue> ENQUEUED = key("openbroker.enqueued");
    /** Messages ever acknowledged. Counter, {@code {message}}. */
    public static final AttributeKey<MetricValue> ACKNOWLEDGED = key("openbroker.acknowledged");
    /** Messages ever expired. Counter, {@code {message}}. */
    public static final AttributeKey<MetricValue> EXPIRED = key("openbroker.expired");
    /** Consumers attached. UpDownCounter, {@code {consumer}}. */
    public static final AttributeKey<MetricValue> CONSUMER_COUNT = key("openbroker.consumer.count");

    /** Subscriptions attached to a destination. UpDownCounter, {@code {subscription}}. */
    public static final AttributeKey<MetricValue> SUBSCRIPTION_COUNT = key("openbroker.subscription.count");
    /** Producers attached. UpDownCounter, {@code {producer}}. */
    public static final AttributeKey<MetricValue> PRODUCER_COUNT = key("openbroker.producer.count");
    /** Channels open on a connection. UpDownCounter, {@code {channel}}. */
    public static final AttributeKey<MetricValue> CHANNEL_COUNT = key("openbroker.channel.count");
    /** Messages ever delivered to a consumer. Counter, {@code {message}}. */
    public static final AttributeKey<MetricValue> DELIVERED = key("openbroker.delivered");
    /** Messages ever sent by a producer. Counter, {@code {message}}. */
    public static final AttributeKey<MetricValue> SENT = key("openbroker.sent");

    private MetricKeys() {}

    private static AttributeKey<MetricValue> key(String name) {
        return AttributeKey.of(name, MetricValue.class);
    }
}
