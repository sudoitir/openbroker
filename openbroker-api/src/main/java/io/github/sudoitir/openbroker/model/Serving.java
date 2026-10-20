package io.github.sudoitir.openbroker.model;

/** Whether a node is serving now, which can differ from its designation after a failover. */
public enum Serving {
    ACTIVE,
    STANDBY,
    UNKNOWN
}
