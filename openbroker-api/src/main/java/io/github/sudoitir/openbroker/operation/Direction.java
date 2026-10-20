package io.github.sudoitir.openbroker.operation;

/** Which flow a pause or resume affects. */
public enum Direction {
    /** Delivery to consumers. */
    DELIVERY,
    /** Messages sent by producers. */
    PUBLISH,
    BOTH
}
