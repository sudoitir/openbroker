package io.github.sudoitir.openbroker.rabbitmq.admin;

import com.rabbitmq.client.ConnectionFactory;
import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.CallScope;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpFactory;
import io.github.sudoitir.openbroker.rabbitmq.amqp.PooledAmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.messaging.RabbitMqMessages;
import io.github.sudoitir.openbroker.rabbitmq.messaging.RabbitMqTransport;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.List;
import java.util.Optional;

/**
 * The {@link ClusterConnection} to one RabbitMQ cluster: wires the facets over one {@link RabbitMqCluster} and one
 * shared AMQP pool. {@code notifications()} is always present, {@code transport()} only with a {@code DATA} endpoint
 * (the facets that need AMQP refuse clearly without one), and {@code unwrap} offers the {@link ManagementClient} and,
 * with a {@code DATA} endpoint, a RabbitMQ {@link ConnectionFactory}. Thread-safe.
 */
public final class RabbitMqConnection implements ClusterConnection {

    private final BrokerSystem system;
    private final RabbitMqCluster cluster;
    private final PooledAmqpConnections connections;
    private final RabbitMqNodes nodes;
    private final RabbitMqResources resources;
    private final RabbitMqOperations operations;
    private final RabbitMqMessages messages;
    private final RabbitMqAccess access;
    private final RabbitMqNotifications notifications;
    private final RabbitMqTransport transport;

    public RabbitMqConnection(
            BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
        this.cluster = new RabbitMqCluster(spec, credentials, context);
        this.connections = new PooledAmqpConnections(cluster);
        this.nodes = new RabbitMqNodes(cluster);
        this.resources = new RabbitMqResources(cluster);
        this.operations = new RabbitMqOperations(cluster);
        this.messages = new RabbitMqMessages(cluster, connections);
        this.access = new RabbitMqAccess(cluster);
        this.notifications = new RabbitMqNotifications(cluster, resources, Optional.of(connections));
        this.transport = new RabbitMqTransport(cluster, connections);
    }

    /**
     * Reaches the cluster once: checks that it is a RabbitMQ the driver supports, describes its nodes and probes what
     * the account may do.
     */
    public ConnectionReport verify() {
        Deadline deadline = Deadlines.current(cluster.options());
        return ScopedValue.where(CallScope.DEADLINE, deadline).call(() -> {
            cluster.features().require(system.supportedVersions());
            Partial<List<NodeDescription>> described = nodes.describe(Scope.whole());
            return new ConnectionReport(nodes.reach(described, deadline), access.probe());
        });
    }

    @Override
    public BrokerSystem system() {
        return system;
    }

    @Override
    public Nodes nodes() {
        return nodes;
    }

    @Override
    public Resources resources() {
        return resources;
    }

    @Override
    public Operations operations() {
        return operations;
    }

    @Override
    public Messages messages() {
        return messages;
    }

    @Override
    public Access access() {
        return access;
    }

    @Override
    public Optional<Notifications> notifications() {
        return Optional.of(notifications);
    }

    @Override
    public Optional<Transport> transport() {
        return hasData() ? Optional.of(transport) : Optional.empty();
    }

    @Override
    public void reconfigure(ClusterSpec spec) {
        cluster.reconfigure(spec);
        notifications.reset();
    }

    /**
     * The management client, and with a {@code DATA} endpoint a new RabbitMQ {@link ConnectionFactory} for the
     * default virtual host. The connection owns the management client; the factory is the caller's. Any other type
     * is absent.
     */
    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        if (type.equals(ManagementClient.class)) {
            return Optional.of(type.cast(cluster.management()));
        }
        if (type.equals(ConnectionFactory.class) && hasData()) {
            return Optional.of(type.cast(new AmqpFactory(cluster).factory("/")));
        }
        return Optional.empty();
    }

    private boolean hasData() {
        ClusterSpec spec = cluster.spec();
        return spec.clusterEndpoints().stream().anyMatch(RabbitMqConnection::isData)
                || spec.nodes().stream()
                        .map(NodeSpec::endpoints)
                        .flatMap(List::stream)
                        .anyMatch(RabbitMqConnection::isData);
    }

    private static boolean isData(Endpoint endpoint) {
        return endpoint.plane() == Plane.DATA;
    }

    @Override
    public void close() {
        notifications.close();
        transport.close();
        messages.close();
        cluster.close();
    }
}
