package io.github.sudoitir.openbroker.messaging;

/** Who counts failed attempts and decides when to give up. */
public enum RetryMode {
    /** The system's own policy applies; the client only reports failure. */
    SYSTEM,
    /** The client counts attempts, delays retries and dead-letters. */
    MANAGED
}
