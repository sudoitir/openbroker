package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Reads permissions and topic permissions from the management API's JSON, and writes them back where the verbs allow. */
final class Permissions {

    private final RabbitMqCluster cluster;

    Permissions(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The permission, or topic permission when it names an exchange, that {@code json} describes. */
    Resource read(JsonNode json) {
        String vhost = json.path("vhost").asString();
        String user = json.path("user").asString();
        boolean topic = json.has("exchange");
        Attributes.Builder attributes = Attributes.builder();
        attributes.put(StandardAttributes.PRINCIPAL, user);
        if (!topic) {
            ReadSupport.put(attributes, RabbitMqKinds.CONFIGURE, ReadSupport.text(json, "configure"));
        }
        ReadSupport.put(attributes, RabbitMqKinds.WRITE, ReadSupport.text(json, "write"));
        ReadSupport.put(attributes, RabbitMqKinds.READ, ReadSupport.text(json, "read"));
        ReadSupport.parent(attributes, vhost);
        ResourceId id = topic
                ? ResourceId.of(
                        RabbitMqKinds.TOPIC_PERMISSION.value(),
                        vhost,
                        user,
                        ManagementPaths.idName(json.path("exchange").asString()))
                : ResourceId.of(RabbitMqKinds.PERMISSION.value(), vhost, user);
        return new Resource(
                id,
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }
}
