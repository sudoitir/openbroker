package io.github.sudoitir.openbroker;

import java.util.Objects;

/**
 * One address a node or a cluster is reached at: its plane, the protocol spoken there (a driver
 * defines the names, such as {@code jolokia} or {@code core}), and the address in that protocol's own
 * syntax ({@code http://host:8161/console/jolokia}, {@code (tcp://a:61616,tcp://b:61616)}).
 */
public record Endpoint(Plane plane, String protocol, String address) {

    public Endpoint {
        Objects.requireNonNull(plane, "plane");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(address, "address");
        if (address.isBlank()) {
            throw new IllegalArgumentException("An endpoint address cannot be blank");
        }
    }

    public static Endpoint of(Plane plane, String protocol, String address) {
        return new Endpoint(plane, protocol, address);
    }
}
