package io.github.sudoitir.openbroker.model;

/** A node's configured role in a high-availability group. */
public enum Designation {
    PRIMARY,
    BACKUP,
    /** Not part of an HA group. */
    NONE
}
