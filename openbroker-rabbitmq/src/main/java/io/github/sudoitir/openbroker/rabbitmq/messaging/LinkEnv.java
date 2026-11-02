package io.github.sudoitir.openbroker.rabbitmq.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqDriver;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpCalls;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.amqp.PooledAmqpConnections;
import io.github.sudoitir.openbroker.support.messaging.EngineContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * What the links of one connection's transport share: the cluster, its AMQP connections, the engine context, the
 * listeners and the open links. It also holds the failures every link builds the same way.
 */
final class LinkEnv implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(LinkEnv.class.getName());

    private final RabbitMqCluster cluster;
    private final AmqpConnections connections;
    private final AmqpCalls calls;
    private final EngineContext engine;
    private final boolean ownsConnections;
    private final CopyOnWriteArrayList<LinkListener> listeners = new CopyOnWriteArrayList<>();
    private final Set<AutoCloseable> open = ConcurrentHashMap.newKeySet();

    /** An environment for one standalone link, with connections of its own that close with the last link. */
    LinkEnv(RabbitMqCluster cluster) {
        this(cluster, new PooledAmqpConnections(cluster), EngineContext.of(cluster.options()), true);
    }

    /** An environment over connections and an engine context that its caller owns. */
    LinkEnv(RabbitMqCluster cluster, AmqpConnections connections, EngineContext engine) {
        this(cluster, connections, engine, false);
    }

    private LinkEnv(RabbitMqCluster cluster, AmqpConnections connections, EngineContext engine, boolean owns) {
        this.cluster = cluster;
        this.connections = connections;
        this.calls = new AmqpCalls(connections, cluster.options().clock());
        this.engine = engine;
        this.ownsConnections = owns;
        if (cluster.options().observer() instanceof LinkListener observer) {
            listeners.add(observer);
        }
    }

    RabbitMqCluster cluster() {
        return cluster;
    }

    AmqpConnections connections() {
        return connections;
    }

    AmqpCalls calls() {
        return calls;
    }

    EngineContext engine() {
        return engine;
    }

    /** A deadline a call to open a link may take, from the connection's call timeout. */
    Deadline deadline() {
        return Deadline.after(
                cluster.options().timeouts().call(), cluster.options().clock());
    }

    /** Hears the state changes of every link until the result is closed. */
    AutoCloseable watch(LinkListener listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    /** Passes one link's event to everyone who watches. */
    void fire(LinkEvent event) {
        for (LinkListener listener : listeners) {
            try {
                listener.changed(event);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "A link listener failed", e);
            }
        }
    }

    /** Remembers a link so closing the environment closes it. */
    <T extends AutoCloseable> T track(T link) {
        open.add(link);
        return link;
    }

    void forget(AutoCloseable link) {
        open.remove(link);
        if (ownsConnections && open.isEmpty()) {
            connections.close();
        }
    }

    /**
     * Runs {@code work} admitted by the caller's interceptor as a data-plane call named {@code operation}, mapping
     * what the AMQP client throws to the SDK's failures.
     */
    <T> T admitted(String operation, ResourceId target, Supplier<T> work) {
        CallInfo call =
                new CallInfo(RabbitMqCluster.CLUSTER, endpoint(), Plane.DATA, operation, 1, Optional.of(target));
        try (Permit permit = cluster.options().interceptor().admit(call)) {
            try {
                T result = work.get();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (RuntimeException e) {
                BrokerException failure = failure(e, Optional.of(target));
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            }
        }
    }

    /** Runs a settlement under the observer's {@code onSettle}, ending it with the settlement's failure, if any. */
    void settling(OperationInfo info, Message message, Disposition disposition, Runnable work) {
        MessagingObserver observer = cluster.options().observer();
        Observation observation =
                observer == MessagingObserver.none() ? null : observer.onSettle(info, message, disposition);
        RuntimeException failure = null;
        try {
            work.run();
        } catch (RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            if (observation != null) {
                try {
                    observation.end(failure);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "An observer failed", e);
                }
            }
        }
    }

    /** The telemetry description of one operation on a destination. */
    OperationInfo info(OperationType type, ResourceId destination, Optional<ResourceId> subscription, int count) {
        String first = endpoint();
        int colon = first.lastIndexOf(':');
        Optional<String> host = colon > 0 ? Optional.of(first.substring(0, colon)) : Optional.empty();
        OptionalInt port = OptionalInt.empty();
        if (colon > 0) {
            try {
                port = OptionalInt.of(Integer.parseInt(first.substring(colon + 1)));
            } catch (NumberFormatException _) {
                // an address without a numeric port is reported without one
            }
        }
        return new OperationInfo(
                RabbitMqDriver.SYSTEM_ID,
                type,
                type.name().toLowerCase(Locale.ROOT),
                destination,
                subscription,
                RabbitMqCluster.CLUSTER,
                host,
                port,
                count);
    }

    /** The first data address, for the interceptor and the telemetry; empty text when the spec names none. */
    String endpoint() {
        return cluster.spec().clusterEndpoints().stream()
                .filter(e -> e.plane() == Plane.DATA)
                .findFirst()
                .or(() -> cluster.spec().nodes().stream()
                        .flatMap(node -> node.endpoints().stream())
                        .filter(e -> e.plane() == Plane.DATA)
                        .findFirst())
                .map(Endpoint::address)
                .orElse("");
    }

    @Override
    public void close() {
        List<AutoCloseable> all = new ArrayList<>(open);
        for (AutoCloseable link : all) {
            try {
                link.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.DEBUG, "A link failed to close", e);
            }
        }
        open.clear();
        if (ownsConnections) {
            connections.close();
        }
    }

    // ---- failures ------------------------------------------------------------------------------

    /** What a failure of a call made for {@code resource} is to the SDK. */
    static BrokerException failure(Throwable e, Optional<ResourceId> resource) {
        if (e instanceof BrokerException broker) {
            return broker;
        }
        if (e instanceof IllegalArgumentException invalid) {
            return RabbitMqErrors.invalid(invalid.getMessage());
        }
        // A channel the broker closed names why, about the resource the call was for.
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ShutdownSignalException shutdown
                    && shutdown.getReason() instanceof AMQP.Channel.Close close) {
                return RabbitMqErrors.fromReply(close.getReplyCode(), close.getReplyText(), resource);
            }
        }
        return RabbitMqErrors.fromIo(e, false);
    }

    /** A settlement of a delivery that no longer exists: its channel is gone, or it is settled already. */
    static BrokerException stale(String message) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.DELIVERY_STALE),
                Dispatch.NOT_SENT,
                RabbitMqCluster.CLUSTER,
                message);
    }

    /** A call the link's state does not allow. */
    static BrokerException precondition(String message) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                Dispatch.NOT_SENT,
                RabbitMqCluster.CLUSTER,
                message);
    }

    /** A link that is down: the work was not sent, and the supervisor reopens the link. */
    static BrokerException down(String what, Throwable cause) {
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.UNREACHABLE),
                Dispatch.NOT_SENT,
                RabbitMqCluster.CLUSTER,
                what + " was lost: "
                        + (cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage()),
                cause);
    }

    /** The node a reopened link says serves it: the cluster name of the connection when it names a node. */
    static NodeId served(Connection connection) {
        try {
            Object name = connection.getServerProperties() == null
                    ? null
                    : connection.getServerProperties().get("cluster_name");
            if (name != null && !name.toString().isBlank()) {
                return NodeId.of(name.toString());
            }
        } catch (RuntimeException _) {
            // an unreadable server property leaves the cluster as the serving node
        }
        return RabbitMqCluster.CLUSTER;
    }

    /** Waits are bounded by the call timeout. */
    Duration callTimeout() {
        return cluster.options().timeouts().call();
    }
}
