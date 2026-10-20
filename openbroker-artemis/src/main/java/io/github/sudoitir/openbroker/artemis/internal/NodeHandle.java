package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.net.URI;
import java.util.Optional;

/** One node's endpoints, as the caller gave them. */
public record NodeHandle(NodeId id, Optional<URI> jolokia, Optional<String> core) {

    public static final String JOLOKIA = "jolokia";
    public static final String CORE = "core";

    public static NodeHandle of(NodeSpec spec) {
        return new NodeHandle(
                spec.id(),
                spec.endpoint(Plane.MANAGEMENT, JOLOKIA).map(e -> URI.create(e.address())),
                spec.endpoint(Plane.DATA, CORE).map(Endpoint::address));
    }
}
