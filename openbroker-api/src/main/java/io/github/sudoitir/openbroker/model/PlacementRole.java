package io.github.sudoitir.openbroker.model;

/** What a node holds of a resource. */
public enum PlacementRole {
    /** An independent copy, as on each node of an Artemis cluster. */
    INSTANCE,
    /** The replica that serves reads and writes. */
    LEADER,
    /** A replica that follows the leader. */
    FOLLOWER
}
