package io.github.sudoitir.openbroker.model;

import java.net.URI;
import java.util.Objects;

/** One end of a route: a resource in this cluster, or something outside it. */
public sealed interface RouteEndpoint {

    record Local(ResourceId resource) implements RouteEndpoint {
        public Local {
            Objects.requireNonNull(resource, "resource");
        }
    }

    /** Outside this cluster: another broker, an HTTP endpoint. */
    record Remote(URI uri, String name) implements RouteEndpoint {
        public Remote {
            Objects.requireNonNull(uri, "uri");
            Objects.requireNonNull(name, "name");
        }
    }
}
