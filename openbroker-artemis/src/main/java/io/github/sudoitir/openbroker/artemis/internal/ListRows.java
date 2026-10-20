package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.model.AttributeKey;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * The rows of Artemis' {@code listX} operations, as resources. Every scalar in a row is a string; a
 * blank one is treated as absent.
 */
public final class ListRows {

    /** How a kind is listed: its operation and how one row becomes a resource, or none to skip it. */
    record Listing(String operation, BiFunction<JsonNode, Attributes.Builder, @Nullable ResourcePath> row) {}

    private static final Map<KindId, Listing> LISTINGS = Map.of(
            ArtemisKinds.QUEUE, new Listing("listQueues", ListRows::queue),
            ArtemisKinds.ADDRESS, new Listing("listAddresses", ListRows::address),
            ArtemisKinds.CONNECTION, new Listing("listConnections", ListRows::connection),
            ArtemisKinds.SESSION, new Listing("listSessions", ListRows::session),
            ArtemisKinds.CONSUMER, new Listing("listConsumers", ListRows::consumer),
            ArtemisKinds.PRODUCER, new Listing("listProducers", ListRows::producer));

    /** {@code Date.toString()}, which most Artemis views use for a creation time. */
    private static final DateTimeFormatter DATE_TO_STRING =
            DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ROOT);

    private ListRows() {}

    /** The listing for a kind, when Artemis lists it with a {@code listX} operation. */
    static Optional<Listing> listing(KindId kind) {
        return Optional.ofNullable(LISTINGS.get(kind));
    }

    public static List<Resource> parse(KindId kind, @Nullable JsonNode data, NodeId node, Instant observedAt) {
        Listing listing = LISTINGS.get(kind);
        List<Resource> out = new ArrayList<>();
        if (listing == null || data == null || !data.isArray()) {
            return out;
        }
        for (JsonNode row : data) {
            Attributes.Builder attributes = Attributes.builder();
            ResourcePath path = listing.row().apply(row, attributes);
            if (path != null) {
                out.add(new Resource(
                        new ResourceId(kind, path),
                        attributes.build(),
                        List.of(),
                        List.of(Placement.instance(node, Attributes.empty())),
                        observedAt));
            }
        }
        return out;
    }

    private static @Nullable ResourcePath queue(JsonNode row, Attributes.Builder a) {
        String name = text(row, "name");
        if (name == null || ArtemisLists.flag(row, "internalQueue")) {
            return null;
        }
        String routingType = text(row, "routingType");
        a.put(StandardAttributes.DURABLE, ArtemisLists.flag(row, "durable"))
                .put(StandardAttributes.PAUSED, ArtemisLists.flag(row, "paused"))
                .putRaw(
                        ArtemisKinds.ROUTING_TYPE,
                        routingType == null ? "ANYCAST" : routingType.toUpperCase(Locale.ROOT));
        metric(a, MetricKeys.BACKLOG, row, "messageCount");
        metric(a, MetricKeys.CONSUMER_COUNT, row, "consumerCount");
        metric(a, MetricKeys.IN_FLIGHT, row, "deliveringCount");
        metric(a, MetricKeys.SCHEDULED, row, "scheduledCount");
        metric(a, MetricKeys.ENQUEUED, row, "messagesAdded");
        metric(a, MetricKeys.ACKNOWLEDGED, row, "messagesAcked");
        metric(a, MetricKeys.EXPIRED, row, "messagesExpired");
        put(a, StandardAttributes.FILTER, text(row, "filter"));
        id(a, StandardAttributes.DESTINATION, ArtemisKinds.ADDRESS, text(row, "address"));
        return ResourcePath.of(name);
    }

    private static @Nullable ResourcePath address(JsonNode row, Attributes.Builder a) {
        String name = text(row, "name");
        if (name == null) {
            return null;
        }
        put(a, ArtemisKinds.ROUTING_TYPES, text(row, "routingTypes"));
        metric(a, MetricKeys.SUBSCRIPTION_COUNT, row, "queueCount");
        metric(a, MetricKeys.BACKLOG, row, "messageCount");
        return ResourcePath.of(name);
    }

    private static @Nullable ResourcePath connection(JsonNode row, Attributes.Builder a) {
        String id = text(row, "connectionID");
        if (id == null) {
            return null;
        }
        client(row, a);
        metric(a, MetricKeys.CHANNEL_COUNT, row, "sessionCount");
        return ResourcePath.of(id);
    }

    private static @Nullable ResourcePath session(JsonNode row, Attributes.Builder a) {
        String id = text(row, "id");
        String connection = text(row, "connectionID");
        if (id == null || connection == null) {
            return null;
        }
        client(row, a);
        id(a, StandardAttributes.PARENT, ArtemisKinds.CONNECTION, connection);
        metric(a, MetricKeys.CONSUMER_COUNT, row, "consumerCount");
        metric(a, MetricKeys.PRODUCER_COUNT, row, "producerCount");
        // A session is named within its connection, which closing it needs.
        return ResourcePath.of(connection, id);
    }

    private static @Nullable ResourcePath consumer(JsonNode row, Attributes.Builder a) {
        String id = text(row, "id");
        String session = text(row, "session");
        if (id == null || session == null) {
            return null;
        }
        client(row, a);
        id(a, StandardAttributes.PARENT, ArtemisKinds.SESSION, session);
        id(a, StandardAttributes.SUBSCRIPTION, ArtemisKinds.QUEUE, text(row, "queue"));
        id(a, StandardAttributes.DESTINATION, ArtemisKinds.ADDRESS, text(row, "address"));
        put(a, StandardAttributes.FILTER, text(row, "filter"));
        put(a, ArtemisKinds.STATUS, text(row, "status"));
        metric(a, MetricKeys.IN_FLIGHT, row, "messagesInTransit");
        metric(a, MetricKeys.DELIVERED, row, "messagesDelivered");
        metric(a, MetricKeys.ACKNOWLEDGED, row, "messagesAcknowledged");
        return ResourcePath.of(session, id);
    }

    private static @Nullable ResourcePath producer(JsonNode row, Attributes.Builder a) {
        String id = text(row, "id");
        String session = text(row, "session");
        if (id == null || session == null) {
            return null;
        }
        client(row, a);
        id(a, StandardAttributes.PARENT, ArtemisKinds.SESSION, session);
        id(a, StandardAttributes.DESTINATION, ArtemisKinds.ADDRESS, text(row, "address"));
        put(a, ArtemisKinds.PRODUCER_NAME, text(row, "name"));
        metric(a, MetricKeys.SENT, row, "msgSent");
        return ResourcePath.of(session, id);
    }

    /** What every client-side row carries: protocol, addresses, client id, principal and creation time. */
    private static void client(JsonNode row, Attributes.Builder a) {
        put(a, StandardAttributes.PROTOCOL, text(row, "protocol"));
        put(a, StandardAttributes.REMOTE_ADDRESS, text(row, "remoteAddress"));
        put(a, ArtemisKinds.LOCAL_ADDRESS, text(row, "localAddress"));
        put(a, StandardAttributes.CLIENT_ID, text(row, "clientID"));
        String user = text(row, "validatedUser");
        put(a, StandardAttributes.PRINCIPAL, user != null ? user : text(row, "user"));
        Instant created = createdAt(text(row, "creationTime"));
        if (created != null) {
            a.put(StandardAttributes.CREATED_AT, created);
        }
    }

    /** A creation time as Artemis writes it: epoch milliseconds, or {@code Date.toString()}. */
    static @Nullable Instant createdAt(@Nullable String text) {
        if (text == null) {
            return null;
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(text));
        } catch (NumberFormatException e) {
            try {
                return ZonedDateTime.parse(text, DATE_TO_STRING).toInstant();
            } catch (DateTimeParseException unparsable) {
                return null;
            }
        }
    }

    private static @Nullable String text(JsonNode row, String field) {
        String value = ArtemisLists.str(row, field);
        return value == null || value.isBlank() ? null : value;
    }

    private static void metric(Attributes.Builder a, AttributeKey<MetricValue> key, JsonNode row, String field) {
        a.put(key, MetricValue.exact(ArtemisLists.num(row, field)));
    }

    private static void put(Attributes.Builder a, AttributeKey<String> key, @Nullable String value) {
        if (value != null) {
            a.put(key, value);
        }
    }

    private static void put(Attributes.Builder a, String key, @Nullable String value) {
        if (value != null) {
            a.putRaw(key, value);
        }
    }

    private static void id(Attributes.Builder a, AttributeKey<ResourceId> key, KindId kind, @Nullable String name) {
        if (name != null) {
            a.put(key, new ResourceId(kind, ResourcePath.of(name)));
        }
    }
}
