package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;
import java.util.Optional;

/**
 * What happens to a message whose handler fails.
 *
 * @param mode who counts attempts
 * @param maxAttempts the attempts before the message is dead-lettered; used by {@code MANAGED}
 * @param backoff the wait between attempts; used by {@code MANAGED}
 * @param deadLetter where a message goes after its last attempt; empty uses the system's dead-letter destination
 */
public record RetryPolicy(RetryMode mode, int maxAttempts, Backoff backoff, Optional<SendTarget> deadLetter) {

    public RetryPolicy {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(backoff, "backoff");
        Objects.requireNonNull(deadLetter, "deadLetter");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
    }

    /** The system's own policy. */
    public static RetryPolicy system() {
        return new RetryPolicy(RetryMode.SYSTEM, 1, Backoff.defaults(), Optional.empty());
    }

    /** The client retries up to {@code maxAttempts} attempts, waiting {@code backoff} between them. */
    public static RetryPolicy managed(int maxAttempts, Backoff backoff) {
        return new RetryPolicy(RetryMode.MANAGED, maxAttempts, backoff, Optional.empty());
    }

    /** This policy, dead-lettering to {@code target}. */
    public RetryPolicy withDeadLetter(SendTarget target) {
        return new RetryPolicy(mode, maxAttempts, backoff, Optional.of(target));
    }
}
