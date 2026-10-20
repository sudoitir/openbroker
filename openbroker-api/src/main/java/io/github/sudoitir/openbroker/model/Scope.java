package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.NodeId;
import java.util.Set;

/** Which part of a resource a call applies to: all of it, or some shards. */
public sealed interface Scope {

    /** The whole resource, on every node and partition. */
    record Whole() implements Scope {}

    record Shards(Set<Shard> shards) implements Scope {
        public Shards {
            shards = Set.copyOf(shards);
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("A scope of shards names at least one");
            }
        }
    }

    Whole WHOLE = new Whole();

    static Scope whole() {
        return WHOLE;
    }

    static Scope node(NodeId node) {
        return new Shards(Set.of(Shard.node(node)));
    }

    static Scope shards(Set<Shard> shards) {
        return new Shards(shards);
    }
}
