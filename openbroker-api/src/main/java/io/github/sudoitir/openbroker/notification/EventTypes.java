package io.github.sudoitir.openbroker.notification;

/** The standard event types. */
public final class EventTypes {

    public static final String RESOURCE_CREATED = "openbroker.resource.created";
    public static final String RESOURCE_DELETED = "openbroker.resource.deleted";
    public static final String CLIENT_CONNECTED = "openbroker.client.connected";
    public static final String CLIENT_DISCONNECTED = "openbroker.client.disconnected";
    public static final String CONSUMER_ATTACHED = "openbroker.consumer.attached";
    public static final String CONSUMER_DETACHED = "openbroker.consumer.detached";
    /** The system itself judged a consumer too slow, by its own policy. */
    public static final String CONSUMER_SLOW = "openbroker.consumer.slow";

    public static final String CHANNEL_OPENED = "openbroker.channel.opened";
    public static final String CHANNEL_CLOSED = "openbroker.channel.closed";
    /** A message was handed to a consumer. */
    public static final String MESSAGE_DELIVERED = "openbroker.message.delivered";
    /** A message ran out of time and left its subscription, to an expiry destination or nowhere. */
    public static final String MESSAGE_EXPIRED = "openbroker.message.expired";

    private EventTypes() {}
}
