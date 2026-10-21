package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/**
 * Who enforces each part of a retry policy on a system.
 *
 * @param delay waiting before a message is delivered again
 * @param attempts counting failed deliveries
 * @param deadLetter moving a message that ran out of attempts
 */
public record RetrySupport(Enforcement delay, Enforcement attempts, Enforcement deadLetter) {

    public RetrySupport {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(attempts, "attempts");
        Objects.requireNonNull(deadLetter, "deadLetter");
    }
}
