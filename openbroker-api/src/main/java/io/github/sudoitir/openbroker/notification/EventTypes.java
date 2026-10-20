package io.github.sudoitir.openbroker.notification;

/** The standard event types. */
public final class EventTypes {

    public static final String RESOURCE_CREATED = "openbroker.resource.created";
    public static final String RESOURCE_DELETED = "openbroker.resource.deleted";
    public static final String CLIENT_CONNECTED = "openbroker.client.connected";
    public static final String CLIENT_DISCONNECTED = "openbroker.client.disconnected";
    public static final String CONSUMER_ATTACHED = "openbroker.consumer.attached";
    public static final String CONSUMER_DETACHED = "openbroker.consumer.detached";

    private EventTypes() {}
}
