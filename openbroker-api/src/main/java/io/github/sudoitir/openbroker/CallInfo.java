package io.github.sudoitir.openbroker;

import io.github.sudoitir.openbroker.model.ResourceId;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * One call a driver is about to make, as its {@link CallInterceptor} sees it.
 *
 * @param node the node called
 * @param endpoint the URI called; callers that limit by endpoint key on it
 * @param plane management or data
 * @param operation a low-cardinality name for the call, fit for a metric tag ({@code read},
 *     {@code exec}, {@code browse}, {@code relay.open})
 * @param cost how many permits the call is worth: a request carrying many operations costs more
 *     than one carrying one, so batching cannot exceed a rate limit
 * @param target the resource the call is about, when there is one
 */
public record CallInfo(
        NodeId node, URI endpoint, Plane plane, String operation, int cost, Optional<ResourceId> target) {

    public CallInfo {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(plane, "plane");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(target, "target");
        if (cost < 0) {
            throw new IllegalArgumentException("cost must not be negative");
        }
    }
}
