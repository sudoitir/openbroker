package io.github.sudoitir.openbroker.operation;

import io.github.sudoitir.openbroker.messaging.Count;
import java.util.Objects;

/** How much an operation would affect, in a unit ({@code message}, {@code resource}). */
public record Estimate(Count count, String unit) {

    public Estimate {
        Objects.requireNonNull(count, "count");
        Objects.requireNonNull(unit, "unit");
    }
}
