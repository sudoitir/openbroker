package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Resource;
import tools.jackson.databind.JsonNode;

/** Reads federation links from the management API's JSON, and writes them back where the verbs allow. */
final class FederationLinks {

    FederationLinks(RabbitMqCluster cluster) {}

    /** The resource that the management API's JSON {@code json} describes. */
    Resource read(JsonNode json) {
        throw RabbitMqErrors.unimplemented("FederationLinks.read");
    }
}
