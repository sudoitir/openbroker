package io.github.sudoitir.openbroker.operation;

/** How a system enforces an {@code ifMatch} precondition. */
public enum PreconditionEnforcement {
    /** In the same atomic step as the change. */
    ATOMIC,
    /**
     * By reading the state, comparing, and then changing it: a change that lands between the read and
     * the operation is not detected.
     */
    CHECKED_BEFORE
}
