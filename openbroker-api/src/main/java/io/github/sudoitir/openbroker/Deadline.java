package io.github.sudoitir.openbroker;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** The moment by which a call must have finished. */
public record Deadline(Instant at) {

    public Deadline {
        Objects.requireNonNull(at, "at");
    }

    public static Deadline after(Duration timeout, Clock clock) {
        return new Deadline(clock.instant().plus(timeout));
    }

    /** What is left of it, never negative. */
    public Duration remaining(Clock clock) {
        Duration left = Duration.between(clock.instant(), at);
        return left.isNegative() ? Duration.ZERO : left;
    }

    public boolean expired(Clock clock) {
        return !clock.instant().isBefore(at);
    }
}
