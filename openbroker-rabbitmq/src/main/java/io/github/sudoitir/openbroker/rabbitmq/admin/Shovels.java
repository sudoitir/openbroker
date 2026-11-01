package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Resource;
import tools.jackson.databind.JsonNode;

/** Reads shovels from the management API's JSON, and writes them back where the verbs allow. */
final class Shovels {

    Shovels(RabbitMqCluster cluster) {}

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        throw RabbitMqErrors.unimplemented("Shovels.read");
    }
}
