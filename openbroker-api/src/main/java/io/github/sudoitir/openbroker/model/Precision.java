package io.github.sudoitir.openbroker.model;

/** Whether a number is exact or derived. */
public enum Precision {
    EXACT,
    /** Derived, such as a backlog computed from offset distances. */
    ESTIMATE
}
