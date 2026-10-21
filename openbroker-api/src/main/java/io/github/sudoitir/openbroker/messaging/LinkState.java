package io.github.sudoitir.openbroker.messaging;

/** Where a link is in its life. */
public enum LinkState {
    /** Usable. */
    OPEN,
    /** Lost, and being reopened. */
    RECOVERING,
    /** Lost, and given up on. */
    FAILED,
    /** Closed by its owner. */
    CLOSED
}
