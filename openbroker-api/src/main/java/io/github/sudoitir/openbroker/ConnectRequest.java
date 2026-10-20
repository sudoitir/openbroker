package io.github.sudoitir.openbroker;

import java.util.Objects;

/** Everything {@link BrokerDrivers#connect} needs: which system, what to reach, as whom, and how. */
public record ConnectRequest(String system, ClusterSpec cluster, CredentialSource credentials, ConnectOptions options) {

    public ConnectRequest {
        Objects.requireNonNull(system, "system");
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(credentials, "credentials");
        Objects.requireNonNull(options, "options");
    }
}
