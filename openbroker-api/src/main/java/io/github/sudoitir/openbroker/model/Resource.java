package io.github.sudoitir.openbroker.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A resource as observed: its identity, its attributes, its health, where it lives, and when the
 * system sampled it ({@code observedAt} is the broker's time where it reports one).
 */
public record Resource(
        ResourceId id,
        Attributes attributes,
        List<Condition> conditions,
        List<Placement> placements,
        Instant observedAt) {

    public Resource {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(attributes, "attributes");
        conditions = List.copyOf(conditions);
        placements = List.copyOf(placements);
        Objects.requireNonNull(observedAt, "observedAt");
    }
}
