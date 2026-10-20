package io.github.sudoitir.openbroker.notification;

import io.github.sudoitir.openbroker.NodeId;
import java.time.Instant;
import java.util.Objects;

/**
 * Some notifications from a node may have been lost since {@code since}: re-read that node's state
 * instead of relying on the stream, as after a Kubernetes watch gap.
 */
public record Gap(NodeId node, Instant since, Cause cause) {

    public enum Cause {
        /** The subscription started; nothing before it was seen. */
        STARTED,
        /** The stream reconnected, and the system does not replay what happened meanwhile. */
        RECONNECTED,
        /** The subscriber fell behind and the driver dropped buffered notifications. */
        OVERFLOW
    }

    public Gap {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(since, "since");
        Objects.requireNonNull(cause, "cause");
    }
}
