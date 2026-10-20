package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Diverts and core bridges, read from their MBeans, as routes. A divert forwards between two
 * addresses of one broker; a bridge links a queue to an address on another broker, named by its
 * connector ({@code urn:artemis:connector:<name>}) since the broker reports no URL for it.
 */
public final class RouteRows {

    /** Native attribute keys a route carries besides the standard ones. */
    public static final String ROUTING_NAME = "artemis.routing_name";

    public static final String EXCLUSIVE = "artemis.exclusive";
    public static final String TRANSFORMER_CLASS = "artemis.transformer_class";
    public static final String TRANSFORMER_PROPERTIES = "artemis.transformer_properties";
    public static final String STATIC_CONNECTORS = "artemis.static_connectors";
    public static final String DISCOVERY_GROUP = "artemis.discovery_group";
    public static final String DUPLICATE_DETECTION = "artemis.duplicate_detection";
    public static final String HA = "artemis.ha";
    public static final String RETRY_INTERVAL = "artemis.retry_interval";
    public static final String RETRY_INTERVAL_MULTIPLIER = "artemis.retry_interval_multiplier";
    public static final String MAX_RETRY_INTERVAL = "artemis.max_retry_interval";
    public static final String RECONNECT_ATTEMPTS = "artemis.reconnect_attempts";

    private RouteRows() {}

    public static @Nullable Resource divert(JsonNode m, NodeId node, Instant observedAt) {
        String name = text(m, "UniqueName");
        String address = text(m, "Address");
        if (name == null || address == null) {
            return null;
        }
        Attributes.Builder a = Attributes.builder()
                .put(StandardAttributes.ROUTE_CLASS, RouteClass.FORWARD)
                .put(StandardAttributes.ROUTE_SOURCE, local(ArtemisKinds.ADDRESS, address))
                .putRaw(EXCLUSIVE, flag(m, "Exclusive"));
        String target = text(m, "ForwardingAddress");
        if (target != null) {
            a.put(StandardAttributes.ROUTE_TARGET, local(ArtemisKinds.ADDRESS, target));
        }
        put(a, StandardAttributes.FILTER.name(), text(m, "Filter"));
        put(a, ROUTING_NAME, text(m, "RoutingName"));
        put(a, ArtemisKinds.ROUTING_TYPE, text(m, "RoutingType"));
        put(a, TRANSFORMER_CLASS, text(m, "TransformerClassName"));
        a.putRaw(TRANSFORMER_PROPERTIES, properties(m));
        return resource(ArtemisKinds.DIVERT, name, a.build(), List.of(), node, observedAt);
    }

    public static @Nullable Resource bridge(JsonNode m, NodeId node, Instant observedAt) {
        String name = text(m, "Name");
        if (name == null) {
            return null;
        }
        List<String> connectors = strings(m, "StaticConnectors");
        String discovery = text(m, "DiscoveryGroupName");
        Attributes.Builder a = Attributes.builder()
                .put(StandardAttributes.ROUTE_CLASS, RouteClass.LINK)
                .putRaw(STATIC_CONNECTORS, connectors)
                .putRaw(DUPLICATE_DETECTION, flag(m, "UseDuplicateDetection"))
                .putRaw(HA, flag(m, "HA"))
                .putRaw(RETRY_INTERVAL, num(m, "RetryInterval"))
                .putRaw(
                        RETRY_INTERVAL_MULTIPLIER,
                        m.path("RetryIntervalMultiplier").asDouble(0))
                .putRaw(MAX_RETRY_INTERVAL, num(m, "MaxRetryInterval"))
                .putRaw(RECONNECT_ATTEMPTS, num(m, "ReconnectAttempts"))
                .putRaw(TRANSFORMER_PROPERTIES, properties(m))
                .put(MetricKeys.ACKNOWLEDGED, MetricValue.exact(num(m, "MessagesAcknowledged")))
                .put(MetricKeys.IN_FLIGHT, MetricValue.exact(num(m, "MessagesPendingAcknowledgement")));
        String queue = text(m, "QueueName");
        if (queue != null) {
            a.put(StandardAttributes.ROUTE_SOURCE, local(ArtemisKinds.QUEUE, queue));
        }
        String remote = !connectors.isEmpty() ? connectors.getFirst() : discovery;
        String forwardTo = text(m, "ForwardingAddress");
        if (remote != null) {
            a.put(
                    StandardAttributes.ROUTE_TARGET,
                    new RouteEndpoint.Remote(connectorUri(remote), forwardTo == null ? "" : forwardTo));
        }
        put(a, DISCOVERY_GROUP, discovery);
        put(a, StandardAttributes.FILTER.name(), text(m, "FilterString"));
        put(a, TRANSFORMER_CLASS, text(m, "TransformerClassName"));
        List<Condition> conditions = List.of(
                condition(ConditionTypes.STARTED, flag(m, "Started")),
                condition(ConditionTypes.CONNECTED, flag(m, "Connected")));
        return resource(ArtemisKinds.BRIDGE, name, a.build(), conditions, node, observedAt);
    }

    /** The opaque identifier of a connector the broker names but gives no URL for. */
    public static URI connectorUri(String connector) {
        return URI.create("urn:artemis:connector:" + URLEncoder.encode(connector, StandardCharsets.UTF_8));
    }

    private static Resource resource(
            KindId kind,
            String name,
            Attributes attributes,
            List<Condition> conditions,
            NodeId node,
            Instant observedAt) {
        return new Resource(
                new ResourceId(kind, ResourcePath.of(name)),
                attributes,
                conditions,
                List.of(Placement.instance(node, Attributes.empty())),
                observedAt);
    }

    private static Condition condition(String type, boolean holds) {
        return new Condition(
                type,
                holds ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                holds ? "Reported" : "NotReported",
                "",
                Optional.empty());
    }

    private static RouteEndpoint local(KindId kind, String name) {
        return new RouteEndpoint.Local(new ResourceId(kind, ResourcePath.of(name)));
    }

    private static Map<String, String> properties(JsonNode m) {
        JsonNode v = m.get("TransformerProperties");
        Map<String, String> out = new LinkedHashMap<>();
        if (v != null && v.isObject()) {
            v.properties().forEach(e -> out.put(e.getKey(), e.getValue().asString()));
        }
        return Map.copyOf(out);
    }

    private static List<String> strings(JsonNode m, String field) {
        JsonNode v = m.get(field);
        List<String> out = new ArrayList<>();
        if (v != null && v.isArray()) {
            v.forEach(e -> out.add(e.asString()));
        }
        return List.copyOf(out);
    }

    private static @Nullable String text(JsonNode m, String field) {
        JsonNode v = m.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        String s = v.asString();
        return s.isBlank() ? null : s;
    }

    private static boolean flag(JsonNode m, String field) {
        JsonNode v = m.get(field);
        return v != null && !v.isNull() && v.asBoolean();
    }

    private static long num(JsonNode m, String field) {
        JsonNode v = m.get(field);
        return v == null || v.isNull() ? 0L : v.asLong();
    }

    private static void put(Attributes.Builder a, String key, @Nullable String value) {
        if (value != null) {
            a.putRaw(key, value);
        }
    }
}
