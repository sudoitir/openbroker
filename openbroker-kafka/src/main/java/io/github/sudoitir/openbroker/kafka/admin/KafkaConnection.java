package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.CallScope;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
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
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;

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
        this(system, new KafkaCluster(spec, credentials, context), context);
    }

    /** A connection over {@code cluster}, whose Admin client a test may count the calls of. */
    public KafkaConnection(BrokerSystem system, KafkaCluster cluster, DriverContext context) {
        this.system = system;
        this.cluster = cluster;
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
        degrading(deadline, cluster::readFeatures).require(system.supportedVersions());
        Partial<List<NodeDescription>> described = degrading(deadline, () -> nodes.describe(Scope.whole()));
        Partial<List<NodeDescription>> reached = cluster.guarded(
                "connection.reach",
                Plane.MANAGEMENT,
                Optional.empty(),
                deadline,
                () -> Reach.of(cluster, described, deadline));
        return new ConnectionReport(reached, access.probe());
    }

    /**
     * Runs a read of the whole cluster. The Admin client sends it to one broker of its choosing, which
     * may be the one that cannot be reached, so the first try gets a quarter of the time left (at most the
     * connect timeout), after which the client's pick falls on another broker and the second try gets
     * what remains, leaving the rest of the call to reach each broker. A cluster whose every broker is gone fails as before.
     */
    private <T> T degrading(Deadline deadline, Supplier<T> read) {
        ConnectOptions options = cluster.options();
        Duration half = deadline.remaining(options.clock()).dividedBy(2);
        Duration first = half.compareTo(options.timeouts().connect()) < 0
                ? half
                : options.timeouts().connect();
        try {
            return ScopedValue.where(CallScope.DEADLINE, Deadline.after(first, options.clock()))
                    .call(read::get);
        } catch (BrokerException e) {
            if (e.status() != Status.UNAVAILABLE && e.status() != Status.DEADLINE_EXCEEDED) {
                throw e;
            }
        }
        return ScopedValue.where(CallScope.DEADLINE, deadline).call(read::get);
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

    /**
     * Kafka's own {@link Admin} client, for what only Kafka has. The connection owns it: closing the
     * returned view does nothing, it follows a {@link #reconfigure}, and its calls are not admitted by
     * the {@code CallInterceptor}. Any other type is absent.
     */
    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        return type.equals(Admin.class) ? Optional.of(type.cast(cluster.nativeAdmin())) : Optional.empty();
    }

    @Override
    public void close() {
        notifications.close();
        transport.close();
        messages.close();
        cluster.close();
    }
}
