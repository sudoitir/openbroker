package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Resource;
import tools.jackson.databind.JsonNode;

/** Reads virtual hosts from the management API's JSON, and writes them back where the verbs allow. */
final class Vhosts {

    Vhosts(RabbitMqCluster cluster) {}

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        throw RabbitMqErrors.unimplemented("Vhosts.read");
    }
}
