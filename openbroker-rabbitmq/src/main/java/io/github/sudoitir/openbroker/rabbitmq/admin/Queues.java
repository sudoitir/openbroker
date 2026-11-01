package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Resource;
import tools.jackson.databind.JsonNode;

/** Reads classic queues, quorum queues and streams from the management API's JSON, and writes them back where the verbs allow. */
final class Queues {

    Queues(RabbitMqCluster cluster) {}

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        throw RabbitMqErrors.unimplemented("Queues.read");
    }
}
