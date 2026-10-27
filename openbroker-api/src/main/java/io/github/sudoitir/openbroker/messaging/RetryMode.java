package io.github.sudoitir.openbroker.messaging;

/** Who counts failed attempts and decides when to give up. */
public enum RetryMode {
    /**
     * Whatever the system enforces: its own policy where it counts attempts, otherwise a managed one
     * with the policy's attempts and backoff. A processor reports what it became in {@code
     * Processor.retry()}.
     */
    DEFAULT,
    /** The system's own policy applies; the client only reports failure. Refused where the system counts no attempts. */
    SYSTEM,
    /** The client counts attempts, delays retries and dead-letters. */
    MANAGED
}
