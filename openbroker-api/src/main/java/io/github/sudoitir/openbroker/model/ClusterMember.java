package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.Endpoint;
import java.util.List;
import java.util.Objects;

/**
 * A member of the cluster as a node knows it, whether or not the connection has it yet: the way to
 * discover a cluster from one seed.
 *
 * @param nativeId the member's own id
 * @param replicas where each of its replicas serves, by designation
 * @param attributes what the system reports about it, namespaced
 */
public record ClusterMember(String nativeId, List<Replica> replicas, Attributes attributes) {

    /** One replica of a member: its designation and where it is reached. */
    public record Replica(Designation designation, Endpoint endpoint) {
        public Replica {
            Objects.requireNonNull(designation, "designation");
            Objects.requireNonNull(endpoint, "endpoint");
        }
    }

    public ClusterMember {
        Objects.requireNonNull(nativeId, "nativeId");
        replicas = List.copyOf(replicas);
        Objects.requireNonNull(attributes, "attributes");
    }
}
