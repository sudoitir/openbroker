package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Objects;

/**
 * An exponential delay between attempts.
 *
 * @param initial the delay before the second attempt
 * @param multiplier how much each delay grows, at least 1
 * @param max the largest delay
 * @param jitter the fraction of a delay, 0 to 1, that is randomised downwards by whoever waits
 */
public record Backoff(Duration initial, double multiplier, Duration max, double jitter) {

    public Backoff {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(max, "max");
        if (initial.isNegative() || initial.isZero()) {
            throw new IllegalArgumentException("initial must be positive");
        }
        if (!(multiplier >= 1.0) || Double.isInfinite(multiplier)) {
            throw new IllegalArgumentException("multiplier must be at least 1");
        }
        if (max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("max must not be less than initial");
        }
        if (!(jitter >= 0.0 && jitter <= 1.0)) {
            throw new IllegalArgumentException("jitter must be between 0 and 1");
        }
    }

    /** 100 ms, doubling, up to 10 s, with 20 % jitter. */
    public static Backoff defaults() {
        return new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(10), 0.2);
    }

    /** The delay before attempt {@code attempt + 1}, without jitter: {@code initial * multiplier^(attempt-1)}, capped at {@code max}. */
    public Duration delay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1");
        }
        double nanos = initial.toNanos() * Math.pow(multiplier, attempt - 1.0);
        double cap = max.toNanos();
        return nanos >= cap ? max : Duration.ofNanos((long) nanos);
    }
}
