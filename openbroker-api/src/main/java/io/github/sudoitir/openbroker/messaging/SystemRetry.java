package io.github.sudoitir.openbroker.messaging;

/**
 * What the messaging system itself enforces when a processor's policy is {@code RetryMode.SYSTEM}.
 * {@code MANAGED} retries are always available, in the client; this says only what the system does on
 * its own.
 *
 * @param delays whether the system waits before it delivers a failed message again
 * @param countsAttempts whether the system counts failed deliveries and gives up after a limit
 * @param deadLetters whether the system moves a message that ran out of attempts to a dead-letter
 *     destination
 */
public record SystemRetry(boolean delays, boolean countsAttempts, boolean deadLetters) {

    /** A system that enforces nothing: every retry is the client's. */
    public static final SystemRetry NONE = new SystemRetry(false, false, false);
}
