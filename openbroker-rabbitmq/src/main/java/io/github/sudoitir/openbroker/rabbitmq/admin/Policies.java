package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;

/** Reads policies and operator policies from the management API's JSON, and writes them back where the verbs allow. */
final class Policies {

    private final RabbitMqCluster cluster;

    Policies(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** The policy that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        return read(RabbitMqKinds.POLICY, json);
    }

    /** The policy or operator policy ({@code kind}) that the management API's JSON {@code json} describes. */
    Resource read(KindId kind, JsonNode json) {
        String vhost = json.path("vhost").asString();
        Attributes.Builder attributes = Attributes.builder();
        ReadSupport.put(attributes, RabbitMqKinds.PATTERN, ReadSupport.text(json, "pattern"));
        ReadSupport.put(attributes, RabbitMqKinds.APPLY_TO, ReadSupport.text(json, "apply-to"));
        ReadSupport.putLong(attributes, RabbitMqKinds.PRIORITY, json, "priority");
        for (Map.Entry<String, JsonNode> entry : json.path("definition").properties()) {
            attributes.putRaw(
                    RabbitMqKinds.DEFINITION_PREFIX + entry.getKey(),
                    entry.getValue().toString());
        }
        ReadSupport.parent(attributes, vhost);
        return new Resource(
                ResourceId.of(kind.value(), vhost, json.path("name").asString()),
                attributes.build(),
                List.of(),
                ReadSupport.onCluster(),
                cluster.options().clock().instant());
    }
}
