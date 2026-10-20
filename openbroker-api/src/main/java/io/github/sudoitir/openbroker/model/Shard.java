package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.NodeId;
import java.util.Objects;

/** A piece of a resource that work can be scoped to: one node's instance, or one partition. */
public sealed interface Shard {

    record NodeInstance(NodeId node) implements Shard {
        public NodeInstance {
            Objects.requireNonNull(node, "node");
        }
    }

    record Partition(PartitionId partition) implements Shard {
        public Partition {
            Objects.requireNonNull(partition, "partition");
        }
    }

    static Shard node(NodeId node) {
        return new NodeInstance(node);
    }

    static Shard partition(PartitionId partition) {
        return new Partition(partition);
    }
}
