package io.github.sudoitir.openbroker;

import java.util.Objects;

/**
 * The caller's stable identifier for one node of a cluster. It is chosen by the caller and never
 * changes when the node restarts; the broker's own identifier is {@code NodeDescription#nativeId}.
 */
public record NodeId(String value) {

    public NodeId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("A node id cannot be blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
