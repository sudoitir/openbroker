package io.github.sudoitir.openbroker.operation;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The closed set of changes to a resource. A kind declares which it supports in its verbs; an
 * operation it does not declare fails with {@code UNIMPLEMENTED}. Operations only one system has
 * are reached through {@code ClusterConnection#unwrap}.
 */
public sealed interface Operation {

    /**
     * Creates the resource with these attributes. Idempotent: creating one that exists with the same
     * attributes succeeds, and one that exists differently fails with {@code ALREADY_EXISTS}.
     */
    record Create(Attributes attributes) implements Operation {
        public Create {
            Objects.requireNonNull(attributes, "attributes");
        }
    }

    /** Merges these writable configuration attributes into the resource. */
    record Update(Attributes patch) implements Operation {
        public Update {
            Objects.requireNonNull(patch, "patch");
        }
    }

    /**
     * Deletes the resource.
     *
     * @param ifEmpty refuse with {@code FAILED_PRECONDITION} when it still holds messages
     * @param disconnectConsumers close attached consumers first instead of refusing
     */
    record Delete(boolean ifEmpty, boolean disconnectConsumers) implements Operation {}

    record Pause(Direction direction) implements Operation {
        public Pause {
            Objects.requireNonNull(direction, "direction");
        }
    }

    record Resume(Direction direction) implements Operation {
        public Resume {
            Objects.requireNonNull(direction, "direction");
        }
    }

    /** Removes every message waiting in a subscription. */
    record Purge() implements Operation {}

    /** Moves a subscription's position. */
    record Seek(Position position) implements Operation {
        public Seek {
            Objects.requireNonNull(position, "position");
        }
    }

    /**
     * Removes the data of a destination or partition up to a position, for every subscription of it.
     * Its effect is wider than a purge.
     */
    record Truncate(Position position) implements Operation {
        public Truncate {
            Objects.requireNonNull(position, "position");
        }
    }

    /**
     * Closes a connection, channel or consumer; on a destination, closes every consumer attached to
     * it.
     */
    record Close() implements Operation {}

    /** Assigns partitions to nodes. */
    record Reassign(Map<PartitionId, List<NodeId>> assignments) implements Operation {
        public Reassign {
            assignments = Map.copyOf(assignments);
        }
    }

    record ElectLeader(Election election) implements Operation {
        public ElectLeader {
            Objects.requireNonNull(election, "election");
        }
    }

    /** Commits or rolls back an in-doubt transaction. */
    record Resolve(Resolution resolution) implements Operation {
        public Resolve {
            Objects.requireNonNull(resolution, "resolution");
        }
    }

    static Operation delete() {
        return new Delete(false, false);
    }

    static Operation purge() {
        return new Purge();
    }
}
