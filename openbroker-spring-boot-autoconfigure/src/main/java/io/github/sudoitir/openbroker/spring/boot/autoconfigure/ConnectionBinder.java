package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.core.env.Environment;

/**
 * Binds one connection from {@code openbroker.connections.<name>} and checks it against its driver,
 * before any network I/O. A connection derived from Spring Boot's connection details is the base the
 * configuration is laid over, key by key.
 */
final class ConnectionBinder {

    static final String PREFIX = "openbroker.connections.";

    private final Environment environment;
    private final BrokerDrivers drivers;

    ConnectionBinder(Environment environment, BrokerDrivers drivers) {
        this.environment = environment;
        this.drivers = drivers;
    }

    Connection bind(String name, @Nullable Connection derived) {
        String prefix = PREFIX + name;
        Connection base = derived == null ? Connection.defaults() : withNoCluster(derived);
        Connection bound = new RecordBinder(environment, name).bind(prefix, Connection.class, base);
        if (derived != null) {
            bound = withCluster(bound, overlay(derived.cluster(), bound.cluster()));
        }
        check(name, bound);
        return bound;
    }

    private void check(String name, Connection connection) {
        String prefix = PREFIX + name;
        String system = connection.system();
        if (system == null || system.isBlank()) {
            throw new ConnectionConfigurationException(name, prefix + ".system", "is required");
        }
        List<String> available =
                drivers.systems().stream().map(BrokerSystem::id).toList();
        if (!available.contains(system)) {
            throw new ConnectionConfigurationException(
                    name,
                    prefix + ".system",
                    "needs the driver for system '" + system + "' on the class path; found drivers for " + available);
        }
        ConnectionSchema schema = drivers.descriptor(system).schema();
        List<ConnectionSchema.Violation> violations =
                schema.validate(connection.cluster().options());
        if (!violations.isEmpty()) {
            ConnectionSchema.Violation first = violations.getFirst();
            throw new ConnectionConfigurationException(
                    name, prefix + ".cluster.options." + first.key(), first.detail());
        }
    }

    private static Connection withNoCluster(Connection connection) {
        return withCluster(connection, Connection.defaults().cluster());
    }

    private static Connection withCluster(Connection connection, ClusterSpec cluster) {
        return new Connection(
                connection.system(),
                cluster,
                connection.credentials(),
                connection.managementCredentials(),
                connection.options(),
                connection.sender(),
                connection.receiver(),
                connection.processor());
    }

    /**
     * The derived cluster with the configured one laid over it: nodes merge by id, and a node's endpoints
     * by plane and protocol; cluster endpoints, when configured, replace the derived ones; options merge
     * by key.
     */
    private static ClusterSpec overlay(ClusterSpec derived, ClusterSpec configured) {
        Map<String, NodeSpec> nodes = new LinkedHashMap<>();
        derived.nodes().forEach(node -> nodes.put(node.id().value(), node));
        for (NodeSpec node : configured.nodes()) {
            NodeSpec existing = nodes.get(node.id().value());
            nodes.put(node.id().value(), existing == null ? node : new NodeSpec(node.id(), merge(existing, node)));
        }
        Map<String, String> options = new LinkedHashMap<>(derived.options());
        options.putAll(configured.options());
        return new ClusterSpec(
                List.copyOf(nodes.values()),
                configured.clusterEndpoints().isEmpty() ? derived.clusterEndpoints() : configured.clusterEndpoints(),
                options);
    }

    private static List<Endpoint> merge(NodeSpec existing, NodeSpec added) {
        List<Endpoint> merged = new ArrayList<>(existing.endpoints());
        for (Endpoint endpoint : added.endpoints()) {
            merged.removeIf(e -> e.plane() == endpoint.plane() && e.protocol().equals(endpoint.protocol()));
            merged.add(endpoint);
        }
        return merged;
    }
}
