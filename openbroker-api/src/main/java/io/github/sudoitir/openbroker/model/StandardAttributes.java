package io.github.sudoitir.openbroker.model;

import java.time.Instant;

/** Standard attribute keys every driver uses for the same meaning. */
public final class StandardAttributes {

    /** A subscription's destination, when it has one. */
    public static final AttributeKey<ResourceId> DESTINATION =
            AttributeKey.of("openbroker.destination", ResourceId.class);
    /** Whether the resource survives a restart. */
    public static final AttributeKey<Boolean> DURABLE = AttributeKey.of("openbroker.durable", Boolean.class);
    /** Whether the resource was created for a client and goes with it. */
    public static final AttributeKey<Boolean> TEMPORARY = AttributeKey.of("openbroker.temporary", Boolean.class);
    /** Whether delivery is paused. */
    public static final AttributeKey<Boolean> PAUSED = AttributeKey.of("openbroker.paused", Boolean.class);
    /** A subscription's or route's filter, as a {@code MessageFilter} expression in the driver's dialect. */
    public static final AttributeKey<String> FILTER = AttributeKey.of("openbroker.filter", String.class);
    /**
     * Where undeliverable messages go: a resource id's text, or {@code none} to keep them where they
     * are, as for a holding queue that must never lose a message to dead-lettering.
     */
    public static final AttributeKey<String> DEAD_LETTER = AttributeKey.of("openbroker.dead_letter", String.class);
    /** Who created a resource for its own use, so others leave it alone. */
    public static final AttributeKey<String> MANAGED_BY = AttributeKey.of("openbroker.managed_by", String.class);
    /** The one node a resource must be created on, for a node-local holding queue. */
    public static final AttributeKey<String> PLACEMENT_NODE =
            AttributeKey.of("openbroker.placement.node", String.class);
    /** A route's source. */
    public static final AttributeKey<RouteEndpoint> ROUTE_SOURCE =
            AttributeKey.of("openbroker.route.source", RouteEndpoint.class);
    /** A route's target. */
    public static final AttributeKey<RouteEndpoint> ROUTE_TARGET =
            AttributeKey.of("openbroker.route.target", RouteEndpoint.class);
    /** What a route does. */
    public static final AttributeKey<RouteClass> ROUTE_CLASS =
            AttributeKey.of("openbroker.route.class", RouteClass.class);
    /** Why a route exists when the system derived it from policy (dead-letter, expiry). */
    public static final AttributeKey<String> DERIVED_FROM = AttributeKey.of("openbroker.derived_from", String.class);
    /** The number of consumers a subscription allows, or -1 for no limit. */
    public static final AttributeKey<Long> MAX_CONSUMERS = AttributeKey.of("openbroker.max_consumers", Long.class);

    /**
     * The resource this one belongs to: a channel's connection, a consumer's or producer's channel, a
     * policy's destination.
     */
    public static final AttributeKey<ResourceId> PARENT = AttributeKey.of("openbroker.parent", ResourceId.class);
    /** The subscription a consumer reads from. */
    public static final AttributeKey<ResourceId> SUBSCRIPTION =
            AttributeKey.of("openbroker.subscription", ResourceId.class);
    /** The wire protocol a client speaks ({@code AMQP}, {@code CORE}, {@code KAFKA}, {@code MQTT}). */
    public static final AttributeKey<String> PROTOCOL = AttributeKey.of("openbroker.protocol", String.class);
    /** Where a client connects from, as the broker sees it. */
    public static final AttributeKey<String> REMOTE_ADDRESS =
            AttributeKey.of("openbroker.remote_address", String.class);
    /** The identifier a client gave itself. */
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.of("openbroker.client_id", String.class);
    /** The authenticated principal a client acts as. */
    public static final AttributeKey<String> PRINCIPAL = AttributeKey.of("openbroker.principal", String.class);
    /** When the resource was created. */
    public static final AttributeKey<Instant> CREATED_AT = AttributeKey.of("openbroker.created_at", Instant.class);

    private StandardAttributes() {}
}
