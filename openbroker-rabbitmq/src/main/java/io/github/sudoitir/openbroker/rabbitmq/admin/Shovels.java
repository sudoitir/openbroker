package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import tools.jackson.databind.JsonNode;

/**
 * Reads shovels from the management API's JSON: the dynamic ones from their runtime parameter, the static ones from
 * the shovel status, with the status beside a parameter when the shovel management plugin reports it. Shovels are
 * read only here.
 */
final class Shovels {

    private static final String STATIC = "static";

    private final RabbitMqCluster cluster;

    Shovels(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The shovel that {@code json} describes: a runtime parameter (it holds a {@code value}) or a status. */
    Resource read(JsonNode json) {
        JsonNode none = cluster.mapper().missingNode();
        return json.has("value") ? read(json, none) : read(none, json);
    }

    /** The shovel that the runtime parameter {@code parameter} defines and the status {@code state} reports. */
    Resource read(JsonNode parameter, JsonNode state) {
        String vhost = ReadSupport.text(parameter, "vhost")
                .or(() -> ReadSupport.text(state, "vhost"))
                .orElse("");
        String name = ReadSupport.text(parameter, "name")
                .or(() -> ReadSupport.text(state, "name"))
                .orElse("");
        JsonNode value = parameter.path("value");
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(StandardAttributes.ROUTE_CLASS, RouteClass.FORWARD);
        attributes.put(
                StandardAttributes.ROUTE_SOURCE,
                endpoint(
                        vhost,
                        uri(value, "src-uri").or(() -> ReadSupport.text(state, "src_uri")),
                        ReadSupport.text(value, "src-queue").or(() -> ReadSupport.text(state, "src_queue")),
                        ReadSupport.text(value, "src-exchange").or(() -> ReadSupport.text(state, "src_exchange"))));
        attributes.put(
                StandardAttributes.ROUTE_TARGET,
                endpoint(
                        vhost,
                        uri(value, "dest-uri").or(() -> ReadSupport.text(state, "dest_uri")),
                        ReadSupport.text(value, "dest-queue").or(() -> ReadSupport.text(state, "dest_queue")),
                        ReadSupport.text(value, "dest-exchange").or(() -> ReadSupport.text(state, "dest_exchange"))));
        if (STATIC.equals(state.path("type").asString())) {
            attributes.put(StandardAttributes.MANAGED_BY, "configuration");
        }
        ReadSupport.parent(attributes, vhost);
        List<Condition> conditions = new ArrayList<>();
        ReadSupport.text(state, "state").ifPresent(s -> conditions.add(ReadSupport.connected(s)));
        List<Placement> placements = ReadSupport.text(state, "node")
                .map(n -> List.of(Placement.instance(NodeId.of(n), Attributes.empty())))
                .orElseGet(ReadSupport::onCluster);
        return new Resource(
                ResourceId.of(RabbitMqKinds.SHOVEL.value(), vhost, name),
                attributes.build(),
                conditions,
                placements,
                cluster.options().clock().instant());
    }

    /** A URI of a shovel definition, which is one text or a list of alternatives (the first is read). */
    private static Optional<String> uri(JsonNode value, String field) {
        JsonNode uri = value.path(field);
        if (uri.isArray()) {
            return uri.isEmpty() ? Optional.empty() : Optional.of(uri.get(0).asString());
        }
        return ReadSupport.text(value, field);
    }

    private RouteEndpoint endpoint(
            String vhost, Optional<String> uri, Optional<String> queue, Optional<String> exchange) {
        if (uri.isPresent() && !ReadSupport.isLocal(uri.get())) {
            return new RouteEndpoint.Remote(
                    ReadSupport.redacted(uri.get()), queue.or(() -> exchange).orElse(""));
        }
        String in = uri.map(u -> ReadSupport.localVhost(u, vhost)).orElse(vhost);
        if (queue.isPresent()) {
            return new RouteEndpoint.Local(
                    ResourceId.of(cluster.queueTypes().kindOf(in, queue.get()).value(), in, queue.get()));
        }
        return new RouteEndpoint.Local(
                exchange.map(e -> ResourceId.of(RabbitMqKinds.EXCHANGE.value(), in, ManagementPaths.idName(e)))
                        .orElseGet(() -> ResourceId.of(RabbitMqKinds.VHOST.value(), in)));
    }
}
