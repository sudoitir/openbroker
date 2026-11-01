package io.github.sudoitir.openbroker.rabbitmq.amqp;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Builds clusters for the AMQP tests without a management API behind them. */
final class AmqpTestClusters {

    private AmqpTestClusters() {}

    static Endpoint data(String protocol, String address) {
        return Endpoint.of(Plane.DATA, protocol, address);
    }

    static RabbitMqCluster cluster(Credentials credentials, ConnectOptions options, Endpoint... clusterEndpoints) {
        return cluster(credentials, options, List.of(clusterEndpoints), List.of());
    }

    static RabbitMqCluster cluster(
            Credentials credentials, ConnectOptions options, List<Endpoint> clusterEndpoints, List<NodeSpec> nodes) {
        return new RabbitMqCluster(
                new ClusterSpec(nodes, clusterEndpoints, Map.of()),
                CredentialSource.of(credentials),
                new Context(options));
    }

    static NodeSpec node(String id, Endpoint... endpoints) {
        return new NodeSpec(NodeId.of(id), List.of(endpoints));
    }

    private record Context(ConnectOptions options) implements DriverContext {
        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return factory.get();
        }

        @Override
        public void evict(String key) {
            // Nothing is kept.
        }
    }
}
