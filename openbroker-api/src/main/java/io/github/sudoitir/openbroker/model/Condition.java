package io.github.sudoitir.openbroker.model;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A health fact in the Kubernetes convention, comparable across systems. Standard types are in
 * {@link ConditionTypes}; native ones are namespaced.
 */
public record Condition(
        String type, ConditionStatus status, String reason, String message, Optional<Instant> lastTransition) {

    public Condition {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(lastTransition, "lastTransition");
    }
}
