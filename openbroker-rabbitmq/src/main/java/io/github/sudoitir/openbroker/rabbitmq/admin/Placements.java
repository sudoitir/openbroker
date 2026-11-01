package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Placement;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Queue placements (leader, followers, instance) and the {@code UNDER_REPLICATED} condition. */
final class Placements {

    private Placements() {}

    /** The placements of the queue of kind {@code kind} that {@code queue} describes. */
    static List<Placement> of(KindId kind, JsonNode queue) {
        throw RabbitMqErrors.unimplemented("Placements.of");
    }
}
