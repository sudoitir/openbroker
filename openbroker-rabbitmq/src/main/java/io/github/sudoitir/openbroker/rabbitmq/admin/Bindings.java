package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.RouteClass;
import io.github.sudoitir.openbroker.model.RouteEndpoint;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Reads queue and exchange bindings from the management API's JSON, and writes them back where the verbs allow. */
final class Bindings {

    private static final String EXCHANGE_DESTINATION = "exchange";
    private static final String EMPTY_KEY = "~";

    private final RabbitMqCluster cluster;

    Bindings(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The kind of binding that {@code json} is: to a queue, or to an exchange. */
    static KindId kind(JsonNode json) {
        return EXCHANGE_DESTINATION.equals(json.path("destination_type").asString())
                ? RabbitMqKinds.EXCHANGE_BINDING
                : RabbitMqKinds.BINDING;
    }

    /** Whether {@code json} binds the default exchange, which the driver leaves out. */
    static boolean fromDefaultExchange(JsonNode json) {
        return json.path("source").asString().isEmpty();
    }

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        KindId kind = kind(json);
        String vhost = json.path("vhost").asString();
        String source = ManagementPaths.idName(json.path("source").asString());
        String destination = json.path("destination").asString();
        String routingKey = json.path("routing_key").asString();
        String key = ReadSupport.text(json, "properties_key").orElse(routingKey.isEmpty() ? EMPTY_KEY : routingKey);
        ResourceId target = RabbitMqKinds.EXCHANGE_BINDING.equals(kind)
                ? ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost, ManagementPaths.idName(destination))
                : ResourceId.of(cluster.queueTypes().kindOf(vhost, destination).value(), vhost, destination);
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(StandardAttributes.ROUTE_CLASS, RouteClass.BINDING);
        attributes.put(
                StandardAttributes.ROUTE_SOURCE,
                new RouteEndpoint.Local(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost, source)));
        attributes.put(StandardAttributes.ROUTE_TARGET, new RouteEndpoint.Local(target));
        if (!routingKey.isEmpty()) {
            attributes.put(StandardAttributes.FILTER, routingKey);
        }
        ReadSupport.members(attributes, RabbitMqKinds.ARGUMENT_PREFIX, json.path("arguments"));
        ReadSupport.parent(attributes, vhost);
        return new Resource(
                ResourceId.of(kind.value(), vhost, source, ManagementPaths.idName(destination), key),
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }
}
