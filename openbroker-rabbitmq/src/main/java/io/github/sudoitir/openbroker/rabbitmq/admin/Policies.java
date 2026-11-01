package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Resource;
import tools.jackson.databind.JsonNode;

/** Reads policies and operator policies from the management API's JSON, and writes them back where the verbs allow. */
final class Policies {

    Policies(RabbitMqCluster cluster) {}

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        throw RabbitMqErrors.unimplemented("Policies.read");
    }
}
