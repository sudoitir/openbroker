package io.github.sudoitir.openbroker.model;

import java.util.Objects;

/** How a metric attribute is measured: its instrument and its UCUM unit ({@code {message}}, {@code By}, {@code s}). */
public record Measure(Instrument instrument, String unit) {

    public Measure {
        Objects.requireNonNull(instrument, "instrument");
        Objects.requireNonNull(unit, "unit");
    }
}
