package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * How a link reopens after it is lost.
 *
 * @param backoff the wait between attempts
 * @param giveUpAfter how long to keep trying before the link is {@code FAILED}; empty never gives up
 */
public record Recovery(Backoff backoff, Optional<Duration> giveUpAfter) {

    public Recovery {
        Objects.requireNonNull(backoff, "backoff");
        Objects.requireNonNull(giveUpAfter, "giveUpAfter");
        if (giveUpAfter.isPresent() && !giveUpAfter.get().isPositive()) {
            throw new IllegalArgumentException("giveUpAfter must be positive");
        }
    }

    /** The default backoff, never giving up. */
    public static Recovery defaults() {
        return new Recovery(Backoff.defaults(), Optional.empty());
    }
}
