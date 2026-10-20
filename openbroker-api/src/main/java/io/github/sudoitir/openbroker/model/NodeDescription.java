package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One node as observed.
 *
 * @param node the caller's id for it
 * @param nativeId the system's own id for it
 * @param endpoints the endpoints it advertises
 * @param serverTime the node's clock when it answered, where the system reports it
 */
public record NodeDescription(
        NodeId node,
        String nativeId,
        Membership membership,
        Set<NodeFunction> functions,
        List<Endpoint> endpoints,
        Optional<String> version,
        List<Condition> conditions,
        Attributes attributes,
        Optional<Instant> serverTime) {

    public NodeDescription {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(nativeId, "nativeId");
        Objects.requireNonNull(membership, "membership");
        functions = Set.copyOf(functions);
        endpoints = List.copyOf(endpoints);
        Objects.requireNonNull(version, "version");
        conditions = List.copyOf(conditions);
        Objects.requireNonNull(attributes, "attributes");
        Objects.requireNonNull(serverTime, "serverTime");
    }
}
