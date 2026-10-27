package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;

/**
 * A resource a listing found, before its details are read: its id, where it lives, and the
 * attributes the listing gave for free. Paging and sorting work on candidates, so a page costs
 * one read of details, not one per resource of the cluster.
 */
record Candidate(ResourceId id, List<Placement> placements, Attributes attributes) {

    Candidate {
        placements = List.copyOf(placements);
    }

    static Candidate of(ResourceId id, Attributes attributes) {
        return new Candidate(id, List.of(), attributes);
    }
}
