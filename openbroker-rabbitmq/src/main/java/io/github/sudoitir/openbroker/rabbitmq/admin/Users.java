package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Reads users from the management API's JSON, and writes them back where the verbs allow. */
final class Users {

    private final RabbitMqCluster cluster;

    Users(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The resource that the management API's JSON {@code json} describes. Its password hash is never read. */
    Resource read(JsonNode json) {
        Attributes.Builder attributes = Attributes.builder();
        attributes.putRaw(
                RabbitMqKinds.TAGS, ReadSupport.tags(json.path("tags")).orElse(""));
        return new Resource(
                ResourceId.of(RabbitMqKinds.USER.value(), json.path("name").asString()),
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }
}
