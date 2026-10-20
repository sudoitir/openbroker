package io.github.sudoitir.openbroker.model;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One metric reading.
 *
 * @param start for a cumulative counter, when it last started from zero; a reset or restart moves
 *     it, so a rate is never computed across one
 * @param breakdown the value per shard, when the system reports it (per partition, per node)
 */
public record MetricValue(double value, Precision precision, Optional<Instant> start, Map<Shard, Double> breakdown) {

    public MetricValue {
        Objects.requireNonNull(precision, "precision");
        Objects.requireNonNull(start, "start");
        breakdown = Map.copyOf(breakdown);
    }

    public static MetricValue exact(double value) {
        return new MetricValue(value, Precision.EXACT, Optional.empty(), Map.of());
    }

    public static MetricValue estimate(double value) {
        return new MetricValue(value, Precision.ESTIMATE, Optional.empty(), Map.of());
    }

    public long asLong() {
        return (long) value;
    }
}
