package io.github.sudoitir.openbroker.model;

import java.util.Objects;

/**
 * A typed attribute name. Standard keys are in {@link StandardAttributes} and {@link MetricKeys};
 * native keys are namespaced by the system id ({@code artemis.routing_type}).
 */
public record AttributeKey<T>(String name, Class<T> type) {

    public AttributeKey {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
    }

    public static <T> AttributeKey<T> of(String name, Class<T> type) {
        return new AttributeKey<>(name, type);
    }
}
