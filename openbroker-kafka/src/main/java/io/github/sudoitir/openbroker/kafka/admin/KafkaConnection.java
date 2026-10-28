package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.kafka.messaging.KafkaTransport;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.List;
import java.util.Optional;

/** One connection to a Kafka cluster. Thread-safe. */
public final class KafkaConnection implements ClusterConnection {

    private final BrokerSystem system;
    private final KafkaCluster cluster;
    private final Topology topology;
    private final KafkaNodes nodes;
    private final Handlers handlers;
    private final KafkaResources resources;
    private final KafkaOperations operations;
    private final KafkaMessages messages;
    private final KafkaNotifications notifications;
    private final KafkaAccess access;
    private final KafkaTransport transport;

    public KafkaConnection(BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
        this.cluster = new KafkaCluster(spec, credentials, context);
        this.topology = new Topology(context.options().clock(), cluster::watchInterval);
        this.nodes = new KafkaNodes(cluster, topology);
        this.handlers = new Handlers(cluster, topology);
        this.resources = new KafkaResources(cluster, handlers);
        this.operations = new KafkaOperations(cluster, handlers);
        this.messages = new KafkaMessages(cluster, handlers);
        this.access = new KafkaAccess(cluster);
        this.notifications = new KafkaNotifications(cluster, resources, handlers);
        this.transport = new KafkaTransport(cluster);
    }

    /**
     * Checks that the cluster is one this driver supports, reads its nodes, reaches every broker on
     * the address it advertises, and reads what the account may do.
     */
    public ConnectionReport verify() {
        Deadline deadline = Deadlines.current(cluster.options());
        cluster.readFeatures().require(system.supportedVersions());
        Partial<List<NodeDescription>> described = nodes.describe(Scope.whole());
        Partial<List<NodeDescription>> reached = cluster.guarded(
                "connection.reach",
                Plane.MANAGEMENT,
                Optional.empty(),
                deadline,
                () -> Reach.of(cluster, described, deadline));
        return new ConnectionReport(reached, access.probe());
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
        return Optional.of(transport);
    }

    @Override
    public void reconfigure(ClusterSpec spec) {
        cluster.reconfigure(spec);
        handlers.forget();
        messages.reset();
        notifications.reset();
    }

    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        return Optional.empty();
    }

    @Override
    public void close() {
        notifications.close();
        transport.close();
        messages.close();
        cluster.close();
    }
}
