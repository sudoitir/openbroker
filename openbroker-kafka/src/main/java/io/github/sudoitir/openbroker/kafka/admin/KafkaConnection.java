package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.kafka.messaging.KafkaTransport;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
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
    private final KafkaAccess access;
    private final KafkaTransport transport;

    public KafkaConnection(BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
        this.cluster = new KafkaCluster(spec, credentials, context);
        this.topology = new Topology(context.options().clock());
        this.nodes = new KafkaNodes(cluster, topology);
        this.handlers = new Handlers(cluster, topology);
        this.resources = new KafkaResources(cluster, handlers);
        this.operations = new KafkaOperations(cluster, handlers);
        this.messages = new KafkaMessages(cluster);
        this.access = new KafkaAccess(cluster);
        this.transport = new KafkaTransport(cluster);
    }

    /**
     * Checks that the cluster is one this driver supports, then reads its nodes and what the account
     * may do.
     */
    public ConnectionReport verify() {
        Deadline deadline = Deadlines.current(cluster.options());
        cluster.call("connection.verify", Optional.empty(), deadline, (admin, await) -> {
            ClusterVersions.require(admin, await, system.supportedVersions());
            return null;
        });
        return new ConnectionReport(nodes.describe(Scope.whole()), access.probe());
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
        return Optional.empty();
    }

    @Override
    public Optional<Transport> transport() {
        // The messaging surface is not wired in yet: KafkaTransport is the seam to return here.
        return Optional.empty();
    }

    @Override
    public void reconfigure(ClusterSpec spec) {
        cluster.reconfigure(spec);
        handlers.forget();
    }

    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        return Optional.empty();
    }

    @Override
    public void close() {
        transport.close();
        cluster.close();
    }
}
