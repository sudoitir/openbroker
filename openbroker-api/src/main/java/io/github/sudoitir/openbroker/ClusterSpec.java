package io.github.sudoitir.openbroker;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What a caller asks a driver to connect to: the nodes it knows, the cluster-level endpoints (a
 * managed service has only these), and the driver's connection options, keyed as in its {@link
 * ConnectionSchema}.
 */
public record ClusterSpec(List<NodeSpec> nodes, List<Endpoint> clusterEndpoints, Map<String, String> options) {

    public ClusterSpec {
        nodes = List.copyOf(nodes);
        clusterEndpoints = List.copyOf(clusterEndpoints);
        options = Map.copyOf(options);
        if (nodes.stream().map(NodeSpec::id).distinct().count() != nodes.size()) {
            throw new IllegalArgumentException("Node ids must be unique");
        }
    }

    public static ClusterSpec ofNodes(List<NodeSpec> nodes) {
        return new ClusterSpec(nodes, List.of(), Map.of());
    }

    public ClusterSpec withOptions(Map<String, String> options) {
        return new ClusterSpec(nodes, clusterEndpoints, options);
    }

    public String option(String key, String fallback) {
        return Objects.requireNonNullElse(options.get(key), fallback);
    }
}
