package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisDriver;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Creating and deleting an Artemis divert on one node: a {@code ROUTE} of class {@code FORWARD} from
 * the address in {@code openbroker.route.source} to the one in {@code openbroker.route.target}.
 *
 * <p>The broker answers 200 and only logs a warning when it declines to deploy a divert, so a create
 * reads the node's diverts before and after: the answer is what was deployed, not what was accepted.
 * A divert is never changed in place; one that exists with a different configuration is refused with
 * the fields that differ.
 */
final class Diverts {

    /** The broker accepted a divert and did not deploy it: a reason only Artemis has. */
    static final String NOT_DEPLOYED = "NOT_DEPLOYED";

    private Diverts() {}

    static OperationResult on(
            Cluster cluster, ResourceId divert, Operation operation, NodeHandle node, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        String broker = client.brokerName(deadline);
        return switch (operation) {
            case Operation.Create c -> create(client, broker, divert, c.attributes(), node, deadline);
            case Operation.Delete d -> delete(client, broker, divert, node, deadline);
            default -> throw Unimplemented.of(operation.getClass().getSimpleName() + " on an Artemis divert");
        };
    }

    private static OperationResult create(
            JolokiaClient client,
            String broker,
            ResourceId divert,
            Attributes attributes,
            NodeHandle node,
            Deadline deadline) {
        Map<String, Object> config = config(divert.name(), attributes);
        Optional<JsonNode> before = read(client, broker, divert, deadline);
        if (before.isPresent()) {
            requireSame(divert, before.get(), config, node);
            return OperationResult.already();
        }
        JolokiaResponse res = client.single(
                JolokiaRequest.exec(
                        broker,
                        "createDivert(java.lang.String)",
                        client.mapper().writeValueAsString(config)),
                deadline,
                Optional.of(divert));
        if (lacksJsonCreate(res)) {
            res = client.single(positionalCreate(broker, config), deadline, Optional.of(divert));
        }
        if (!res.ok()) {
            BrokerException refused = Errors.refusal(node.id(), res, "createDivert");
            if (!ErrorReasons.RESOURCE_EXISTS.equals(refused.reason())) {
                throw refused;
            }
        }
        JsonNode after = read(client, broker, divert, deadline)
                .orElseThrow(() -> new BrokerException(
                        Status.FAILED_PRECONDITION,
                        new ErrorInfo(NOT_DEPLOYED, ArtemisDriver.SYSTEM, Map.of()),
                        Dispatch.REJECTED,
                        node.id(),
                        "The broker accepted divert " + divert.name() + " but did not deploy it. Its log says why;"
                                + " the usual cause is an existing binding with the same routing name."));
        requireSame(divert, after, config, node);
        return OperationResult.done();
    }

    private static OperationResult delete(
            JolokiaClient client, String broker, ResourceId divert, NodeHandle node, Deadline deadline) {
        if (read(client, broker, divert, deadline).isEmpty()) {
            return OperationResult.already();
        }
        JolokiaResponse res = client.single(
                JolokiaRequest.exec(broker, "destroyDivert(java.lang.String)", divert.name()),
                deadline,
                Optional.of(divert));
        if (res.ok()) {
            return OperationResult.done();
        }
        BrokerException refused = Errors.refusal(node.id(), res, "destroyDivert");
        if (ErrorReasons.RESOURCE_GONE.equals(refused.reason())) {
            return OperationResult.already();
        }
        throw refused;
    }

    /** The divert's MBean attributes on this node, or empty when it has none by that name. */
    private static Optional<JsonNode> read(JolokiaClient client, String broker, ResourceId divert, Deadline deadline) {
        for (String mbean : client.search(ArtemisMBeans.divertsPattern(broker), deadline)) {
            JolokiaResponse res = client.single(JolokiaRequest.readAll(mbean), deadline, Optional.of(divert));
            if (res.ok() && res.value() != null && divert.name().equals(text(res.value(), "UniqueName"))) {
                return Optional.of(res.value());
            }
        }
        return Optional.empty();
    }

    /** A {@code DivertConfiguration} document: unset keys are left out, so the broker's defaults stand. */
    static Map<String, Object> config(String name, Attributes attributes) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("name", name);
        config.put(
                "routing-name",
                attributes.raw(RouteRows.ROUTING_NAME).map(String::valueOf).orElse(name));
        config.put("address", address(attributes.get(StandardAttributes.ROUTE_SOURCE), "source"));
        config.put("forwarding-address", address(attributes.get(StandardAttributes.ROUTE_TARGET), "target"));
        config.put(
                "exclusive",
                attributes
                        .raw(RouteRows.EXCLUSIVE)
                        .map(v -> Boolean.parseBoolean(String.valueOf(v)))
                        .orElse(false));
        attributes
                .get(StandardAttributes.FILTER)
                .filter(f -> !f.isBlank())
                .ifPresent(f -> config.put("filter-string", f));
        attributes
                .raw(ArtemisKinds.ROUTING_TYPE)
                .map(String::valueOf)
                .filter(t -> !t.isBlank())
                .ifPresent(t -> config.put("routing-type", t.toUpperCase(Locale.ROOT)));
        attributes.raw(RouteRows.TRANSFORMER_CLASS).map(String::valueOf).ifPresent(className -> {
            Map<String, Object> transformer = new LinkedHashMap<>();
            transformer.put("class-name", className);
            attributes.raw(RouteRows.TRANSFORMER_PROPERTIES).ifPresent(p -> transformer.put("properties", p));
            config.put("transformer-configuration", transformer);
        });
        return config;
    }

    private static String address(Optional<RouteEndpoint> endpoint, String end) {
        if (endpoint.isPresent() && endpoint.get() instanceof RouteEndpoint.Local(ResourceId resource)) {
            return resource.name();
        }
        throw Queries.invalid("An Artemis divert's " + end + " is a local address.");
    }

    /**
     * Artemis before 2.38 has no JSON {@code createDivert(String)} and says so with this error; the
     * positional arm carries the same fields.
     */
    static boolean lacksJsonCreate(JolokiaResponse res) {
        return !res.ok() && res.error() != null && res.error().contains("No operation createDivert(java.lang.String)");
    }

    /**
     * The positional arm older brokers have. Jolokia cannot send a null argument, so an absent filter
     * and transformer class go as empty strings, which the broker reads as none; the properties go as a
     * map, because the overload taking them as JSON keeps each value's quotes on 2.33. An absent routing
     * type is the broker's own default, {@code STRIP}.
     */
    static JolokiaRequest positionalCreate(String broker, Map<String, Object> config) {
        String transformerClass = "";
        Object transformerProperties = Map.of();
        if (config.get("transformer-configuration") instanceof Map<?, ?> transformer) {
            transformerClass = String.valueOf(transformer.get("class-name"));
            if (transformer.get("properties") != null) {
                transformerProperties = transformer.get("properties");
            }
        }
        return JolokiaRequest.exec(
                broker,
                "createDivert(java.lang.String,java.lang.String,java.lang.String,java.lang.String,boolean,"
                        + "java.lang.String,java.lang.String,java.util.Map,java.lang.String)",
                config.get("name"),
                config.get("routing-name"),
                config.get("address"),
                config.get("forwarding-address"),
                Boolean.TRUE.equals(config.get("exclusive")),
                config.getOrDefault("filter-string", ""),
                transformerClass,
                transformerProperties,
                config.getOrDefault("routing-type", "STRIP"));
    }

    private static void requireSame(ResourceId divert, JsonNode deployed, Map<String, Object> config, NodeHandle node) {
        List<String> differing = differences(deployed, config);
        if (!differing.isEmpty()) {
            throw new BrokerException(
                    Status.ALREADY_EXISTS,
                    ErrorInfo.of(ErrorReasons.RESOURCE_EXISTS, Map.of("differing", String.join(",", differing))),
                    Dispatch.REJECTED,
                    node.id(),
                    "Divert " + divert.name() + " exists here with a different " + String.join(", ", differing)
                            + ". Delete it first; a divert is never changed in place.");
        }
    }

    /** The configuration keys on which a deployed divert differs from the requested one. */
    static List<String> differences(JsonNode deployed, Map<String, Object> config) {
        List<String> differing = new ArrayList<>();
        compare(differing, "address", text(deployed, "Address"), config.get("address"));
        compare(differing, "forwarding-address", text(deployed, "ForwardingAddress"), config.get("forwarding-address"));
        compare(differing, "routing-name", text(deployed, "RoutingName"), config.get("routing-name"));
        compare(differing, "filter-string", text(deployed, "Filter"), config.get("filter-string"));
        compare(differing, "exclusive", text(deployed, "Exclusive"), config.get("exclusive"));
        // Unset means the broker's default, which is not compared rather than guessed.
        if (config.containsKey("routing-type")) {
            compare(differing, "routing-type", text(deployed, "RoutingType"), config.get("routing-type"));
        }
        if (config.get("transformer-configuration") instanceof Map<?, ?> transformer) {
            compare(
                    differing,
                    "transformer-class-name",
                    text(deployed, "TransformerClassName"),
                    transformer.get("class-name"));
        }
        return differing;
    }

    private static void compare(List<String> differing, String key, @Nullable String actual, @Nullable Object wanted) {
        String a = actual == null || actual.isBlank() ? null : actual;
        String w = wanted == null || String.valueOf(wanted).isBlank() ? null : String.valueOf(wanted);
        if (!Objects.equals(a, w)) {
            differing.add(key);
        }
    }

    private static @Nullable String text(JsonNode m, String field) {
        JsonNode v = m.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }
}
