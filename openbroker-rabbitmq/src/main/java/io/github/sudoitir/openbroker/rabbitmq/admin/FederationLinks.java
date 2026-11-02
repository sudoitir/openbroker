package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.List;
import java.util.Optional;
import tools.jackson.databind.JsonNode;

/** Reads federation links from the management API's JSON. Links are read only: policies and upstreams make them. */
final class FederationLinks {

    private static final String QUEUE = "queue";

    private final RabbitMqCluster cluster;

    FederationLinks(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        String vhost = json.path("vhost").asString();
        String type = json.path("type").asString();
        boolean queue = QUEUE.equals(type);
        String local = json.path(queue ? QUEUE : "exchange").asString();
        String upstream = json.path("upstream").asString();
        String remote =
                json.path(queue ? "upstream_queue" : "upstream_exchange").asString();
        ResourceId target = queue
                ? ResourceId.of(cluster.queueTypes().kindOf(vhost, local).value(), vhost, local)
                : ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost, ManagementPaths.idName(local));
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(StandardAttributes.ROUTE_CLASS, RouteClass.LINK);
        attributes.put(
                StandardAttributes.ROUTE_SOURCE,
                new RouteEndpoint.Remote(ReadSupport.redacted(json.path("uri").asString()), remote));
        attributes.put(StandardAttributes.ROUTE_TARGET, new RouteEndpoint.Local(target));
        Optional<String> policy = ReadSupport.text(json, "policy");
        attributes.put(
                StandardAttributes.DERIVED_FROM,
                policy.map(p -> "policy " + p + ", upstream " + upstream).orElse("upstream " + upstream));
        ReadSupport.parent(attributes, vhost);
        List<Placement> placements = ReadSupport.text(json, "node")
                .map(n -> List.of(Placement.instance(NodeId.of(n), Attributes.empty())))
                .orElseGet(ReadSupport::onCluster);
        return new Resource(
                ResourceId.of(RabbitMqKinds.FEDERATION_LINK.value(), vhost, upstream, type, local),
                attributes.build(),
                ReadSupport.text(json, "status")
                        .map(s -> List.of(ReadSupport.connected(s)))
                        .orElse(List.of()),
                placements,
                cluster.options().clock().instant());
    }
}
