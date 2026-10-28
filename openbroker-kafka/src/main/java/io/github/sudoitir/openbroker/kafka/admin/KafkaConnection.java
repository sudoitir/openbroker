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
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.kafka.messaging.KafkaTransport;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Serving;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
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
     * Checks that the cluster is one this driver supports, reads its nodes, reaches every broker on the
     * address it advertises, and reads what the account may do.
     *
     * <p>It returns a result whenever one broker answers. The reads of the whole cluster (what it is, its
     * nodes) get at most half of the deadline, and each broker is then reached on a client of its own in the
     * rest. A read that gets no answer in its share leaves the cluster's features unknown and its nodes
     * those the caller named, each of which is reached or failed on its own; the call fails only when no
     * broker answers at all.
     */
    public ConnectionReport verify() {
        Deadline deadline = Deadlines.current(cluster.options());
        return ScopedValue.where(CallScope.DEADLINE, deadline).call(() -> verifyWithin(deadline));
    }

    private ConnectionReport verifyWithin(Deadline deadline) {
        ConnectOptions options = cluster.options();
        Deadline reads = Deadline.after(deadline.remaining(options.clock()).dividedBy(2), options.clock());
        BrokerException unanswered = null;
        try {
            degrading(reads, cluster::readFeatures).require(system.supportedVersions());
        } catch (BrokerException e) {
            if (!silent(e)) {
                throw e;
            }
            unanswered = e;
        }
        Partial<List<NodeDescription>> described;
        try {
            described = degrading(reads, () -> nodes.describe(Scope.whole()));
        } catch (BrokerException e) {
            if (!silent(e)) {
                throw e;
            }
            unanswered = e;
            described = declared();
        }
        Partial<List<NodeDescription>> known = described;
        Partial<List<NodeDescription>> reached = cluster.guarded(
                "connection.reach",
                Plane.MANAGEMENT,
                Optional.empty(),
                deadline,
                () -> Reach.of(cluster, known, deadline));
        if (unanswered != null && reached.outcomes().values().stream().noneMatch(o -> o == NodeOutcome.APPLIED)) {
            throw unanswered;
        }
        return new ConnectionReport(reached, access.probe());
    }

    private static boolean silent(BrokerException e) {
        return e.status() == Status.UNAVAILABLE || e.status() == Status.DEADLINE_EXCEEDED;
    }

    /** The nodes the caller named, each as an unread broker, for a cluster that would not describe itself. */
    private Partial<List<NodeDescription>> declared() {
        List<NodeDescription> out = new ArrayList<>();
        for (NodeSpec spec : cluster.declaredNodes()) {
            out.add(new NodeDescription(
                    spec.id(),
                    spec.id().value(),
                    Membership.standalone(Serving.ACTIVE),
                    EnumSet.of(NodeFunction.BROKER),
                    spec.endpoints(),
                    Optional.empty(),
                    List.of(new Condition(
                            ConditionTypes.SERVING,
                            ConditionStatus.UNKNOWN,
                            "NOT_READ",
                            "The cluster did not describe itself in time.",
                            Optional.empty())),
                    Attributes.empty(),
                    Optional.empty()));
        }
        return Partial.complete(out, out.stream().map(NodeDescription::node).toList());
    }

    /**
     * Runs a read of the whole cluster. The Admin client sends it to one broker of its choosing, which
     * may be the one that cannot be reached, so the first try gets a quarter of the time left (at most
     * the connect timeout), after which the client's pick falls on another broker and the second try gets
     * what remains of {@code deadline}.
     */
    private <T> T degrading(Deadline deadline, Supplier<T> read) {
        ConnectOptions options = cluster.options();
        Duration quarter = deadline.remaining(options.clock()).dividedBy(4);
        Duration first = quarter.compareTo(options.timeouts().connect()) < 0
                ? quarter
                : options.timeouts().connect();
        try {
            return ScopedValue.where(CallScope.DEADLINE, Deadline.after(first, options.clock()))
                    .call(read::get);
        } catch (BrokerException e) {
            if (!silent(e)) {
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
