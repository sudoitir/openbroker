package io.github.sudoitir.openbroker.model;

/** The OpenTelemetry instrument a metric attribute behaves like. */
public enum Instrument {
    /** Only grows, until a reset; rates are derived from it. */
    COUNTER,
    /** Goes up and down; its value is the current amount. */
    UP_DOWN_COUNTER,
    /** A sampled value that is not summed. */
    GAUGE
}
