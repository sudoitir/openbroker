package io.github.sudoitir.openbroker.model;

/** The Kubernetes spec/status split, plus metrics. */
public enum AttributeCategory {
    /** Desired configuration the caller may set. */
    CONFIG,
    /** Observed state the system reports. */
    STATUS,
    /** A number measured over time. */
    METRIC
}
