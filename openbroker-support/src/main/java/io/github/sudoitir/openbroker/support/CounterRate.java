package io.github.sudoitir.openbroker.support;

import io.github.sudoitir.openbroker.model.MetricValue;
import java.time.Duration;
import java.time.Instant;
import java.util.OptionalDouble;

/**
 * The rate of a cumulative counter between two readings, never across a reset. A counter that went
 * backwards, or whose start moved, was reset or restarted; that interval's rate is unknown, never
 * zero and never negative.
 */
public final class CounterRate {

    private CounterRate() {}

    /** Per second, or empty when it cannot be known. */
    public static OptionalDouble perSecond(MetricValue before, Instant beforeAt, MetricValue after, Instant afterAt) {
        Duration elapsed = Duration.between(beforeAt, afterAt);
        if (elapsed.isZero() || elapsed.isNegative()) {
            return OptionalDouble.empty();
        }
        if (before.start().isPresent()
                && after.start().isPresent()
                && !before.start().equals(after.start())) {
            return OptionalDouble.empty();
        }
        double delta = after.value() - before.value();
        if (delta < 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(delta / (elapsed.toNanos() / 1e9));
    }
}
