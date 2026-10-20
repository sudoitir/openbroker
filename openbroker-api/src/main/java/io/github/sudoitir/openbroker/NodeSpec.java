package io.github.sudoitir.openbroker;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One node the caller wants to reach, with every endpoint it knows for it. */
public record NodeSpec(NodeId id, List<Endpoint> endpoints) {

    public NodeSpec {
        Objects.requireNonNull(id, "id");
        endpoints = List.copyOf(endpoints);
    }

    /** The first endpoint of this plane and protocol. */
    public Optional<Endpoint> endpoint(Plane plane, String protocol) {
        return endpoints.stream()
                .filter(e -> e.plane() == plane && e.protocol().equals(protocol))
                .findFirst();
    }
}
