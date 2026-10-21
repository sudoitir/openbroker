package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.messaging.Backoff;
import java.time.Duration;
import java.util.random.RandomGenerator;

/** Backoff delays with jitter. */
public final class Backoffs {

    private Backoffs() {}

    /**
     * The wait before attempt {@code attempt} (1-based): {@link Backoff#delay} spread by {@code
     * jitter} to either side, and never above {@link Backoff#max()}.
     */
    public static Duration delay(Backoff backoff, int attempt, RandomGenerator random) {
        Duration base = backoff.delay(attempt);
        if (backoff.jitter() == 0.0) {
            return base;
        }
        double factor = 1.0 - backoff.jitter() + 2.0 * backoff.jitter() * random.nextDouble();
        return Duration.ofNanos(
                Math.min((long) (base.toNanos() * factor), backoff.max().toNanos()));
    }
}
