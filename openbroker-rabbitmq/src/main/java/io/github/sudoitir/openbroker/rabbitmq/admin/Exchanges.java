package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Delivery;
import io.github.sudoitir.openbroker.model.DeliveryModes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Reads exchanges from the management API's JSON, and writes them back where the verbs allow. */
final class Exchanges {

    private final RabbitMqCluster cluster;

    Exchanges(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        String vhost = json.path("vhost").asString();
        String name = ManagementPaths.idName(json.path("name").asString());
        Attributes.Builder attributes = Attributes.builder();
        ReadSupport.put(attributes, RabbitMqKinds.EXCHANGE_TYPE, ReadSupport.text(json, "type"));
        attributes.put(StandardAttributes.DURABLE, ReadSupport.flag(json, "durable"));
        attributes.putRaw(RabbitMqKinds.AUTO_DELETE, ReadSupport.flag(json, "auto_delete"));
        attributes.putRaw(RabbitMqKinds.INTERNAL, ReadSupport.flag(json, "internal"));
        attributes.put(
                StandardAttributes.DELIVERY,
                ManagementPaths.DEFAULT_EXCHANGE.equals(name)
                        ? DeliveryModes.of(Delivery.POINT_TO_POINT)
                        : DeliveryModes.of(Delivery.PUBLISH_SUBSCRIBE));
        ReadSupport.members(attributes, RabbitMqKinds.ARGUMENT_PREFIX, json.path("arguments"));
        ReadSupport.parent(attributes, vhost);
        return new Resource(
                ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost, name),
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }
}
