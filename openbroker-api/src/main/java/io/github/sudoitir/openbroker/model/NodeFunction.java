package io.github.sudoitir.openbroker.model;

/** What a node does. A node may do several. */
public enum NodeFunction {
    /** Serves clients and holds messages. */
    BROKER,
    /** Coordinates the cluster: a KRaft controller. */
    CONTROLLER,
    /** Stores data for brokers: a BookKeeper bookie. */
    STORAGE
}
