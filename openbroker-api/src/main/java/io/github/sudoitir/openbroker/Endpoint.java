package io.github.sudoitir.openbroker;

import java.net.URI;
import java.util.Objects;

/**
 * One address a node or a cluster is reached at: its plane, the protocol spoken there (a driver
 * defines the names, such as {@code jolokia} or {@code core}), and the URI.
 */
public record Endpoint(Plane plane, String protocol, URI uri) {

    public Endpoint {
        Objects.requireNonNull(plane, "plane");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(uri, "uri");
    }

    public static Endpoint of(Plane plane, String protocol, String uri) {
        return new Endpoint(plane, protocol, URI.create(uri));
    }
}
