package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;
import java.util.Optional;
import tools.jackson.databind.JsonNode;

/** Reads virtual hosts from the management API's JSON, and writes them back where the verbs allow. */
final class Vhosts {

    private final RabbitMqCluster cluster;

    Vhosts(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        JsonNode metadata = json.path("metadata");
        Attributes.Builder attributes = Attributes.builder();
        ReadSupport.put(attributes, RabbitMqKinds.DESCRIPTION, field(json, metadata, "description"));
        ReadSupport.put(attributes, RabbitMqKinds.DEFAULT_QUEUE_TYPE, field(json, metadata, "default_queue_type"));
        JsonNode tags = json.has("tags") ? json.path("tags") : metadata.path("tags");
        ReadSupport.put(attributes, RabbitMqKinds.TAGS, ReadSupport.tags(tags));
        return new Resource(
                ResourceId.of(RabbitMqKinds.VHOST.value(), json.path("name").asString()),
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }

    private static Optional<String> field(JsonNode json, JsonNode metadata, String name) {
        return ReadSupport.text(json, name).or(() -> ReadSupport.text(metadata, name));
    }
}
