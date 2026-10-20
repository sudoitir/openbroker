package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.NodeId;
import java.util.Objects;
import java.util.Optional;

/** Where a resource lives: the node, the partition, the role there, and whether that replica is in sync. */
public record Placement(
        NodeId node,
        Optional<PartitionId> partition,
        PlacementRole role,
        Optional<Boolean> inSync,
        Attributes attributes) {

    public Placement {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(inSync, "inSync");
        Objects.requireNonNull(attributes, "attributes");
    }

    public static Placement instance(NodeId node, Attributes attributes) {
        return new Placement(node, Optional.empty(), PlacementRole.INSTANCE, Optional.empty(), attributes);
    }
}
