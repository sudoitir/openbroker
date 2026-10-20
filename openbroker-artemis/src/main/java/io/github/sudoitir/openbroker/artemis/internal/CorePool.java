package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Session;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

/**
 * Pooled Core connections, one per node and TLS material, for short browse and send sessions. A
 * connection is opened on first use and closed with the connection that owns the pool.
 *
 * <ul>
 *   <li>{@code useTopologyForLoadBalancing=false}: the broker pushes its topology, whose connector
 *       hosts are often unresolvable from the client, and a blocking call would wedge reconnecting
 *       to one.
 *   <li>One connect attempt and no reconnect: a node that is down fails the call, and the caller
 *       decides what to do.
 *   <li>A 64 KiB consumer window: a browse of one page never buffers more.
 * </ul>
 */
final class CorePool implements AutoCloseable {

    /** Bytes a consumer or browser buffers ahead of reading. */
    static final int CONSUMER_WINDOW_BYTES = 64 * 1024;

    /** Short-lived browse and send sessions per node. */
    static final int SESSIONS = 8;

    private final Cluster cluster;
    private final Map<String, JmsPoolConnectionFactory> pools = new ConcurrentHashMap<>();

    CorePool(Cluster cluster) {
        this.cluster = cluster;
    }

    /** What a Core call does with its session. */
    @FunctionalInterface
    interface Work<T> {
        T run(Session session) throws JMSException;
    }

    /** Runs one call on a pooled session of the node, admitted by the caller's interceptor. */
    <T> T call(NodeHandle node, String operation, Optional<ResourceId> target, Deadline deadline, Work<T> work) {
        String endpoint = node.core()
                .orElseThrow(() -> new BrokerException(
                        Status.FAILED_PRECONDITION,
                        ErrorInfo.of(ErrorReasons.ENDPOINT_NOT_FOUND),
                        Dispatch.NOT_SENT,
                        node.id(),
                        "This node has no Core endpoint."));
        if (deadline.expired(cluster.options().clock())) {
            throw Deadlines.exceeded(node.id(), Dispatch.NOT_SENT);
        }
        try (Permit permit = cluster.options()
                .interceptor()
                .admit(new CallInfo(node.id(), endpoint, Plane.DATA, operation, 1, target))) {
            try {
                T result = withSession(node, endpoint, work);
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        }
    }

    private <T> T withSession(NodeHandle node, String endpoint, Work<T> work) {
        Optional<TlsMaterial> tls = cluster.options().tls().current();
        String key = endpoint + "|" + tls.map(TlsMaterial::id).orElse("plain");
        JmsPoolConnectionFactory pool = pools.computeIfAbsent(key, k -> build(endpoint, tls));
        Credentials account = cluster.credentials().forPlane(Plane.DATA);
        Connection connection = null;
        try {
            connection = account.isNone()
                    ? pool.createConnection()
                    : pool.createConnection(
                            account.username(), account.password().reveal());
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                return work.run(session);
            }
        } catch (JMSException e) {
            throw failure(node, e);
        } finally {
            if (connection != null) {
                try {
                    // Pooled: this returns the connection to the pool, never closes the socket.
                    connection.close();
                } catch (JMSException e) {
                    // teardown
                }
            }
        }
    }

    private JmsPoolConnectionFactory build(String endpoint, Optional<TlsMaterial> tls) {
        ConnectOptions options = cluster.options();
        StringBuilder url = new StringBuilder(endpoint)
                .append(endpoint.contains("?") ? ";" : "?")
                .append("useTopologyForLoadBalancing=false;consumerWindowSize=")
                .append(CONSUMER_WINDOW_BYTES)
                .append(";connectTimeoutMillis=")
                .append(options.timeouts().connect().toMillis());
        tls.ifPresent(material -> {
            url.append(";sslEnabled=true;")
                    .append(TransportConstants.SSL_CONTEXT_PROP_NAME)
                    .append('=')
                    .append(CoreTls.register(material))
                    .append(";verifyHost=")
                    .append(material.hostnameVerification());
            if (!material.protocols().isEmpty()) {
                url.append(";enabledProtocols=").append(String.join(",", material.protocols()));
            }
            if (!material.cipherSuites().isEmpty()) {
                url.append(";enabledCipherSuites=").append(String.join(",", material.cipherSuites()));
            }
        });
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url.toString());
        long call = options.timeouts().call().toMillis();
        factory.setInitialConnectAttempts(1);
        factory.setReconnectAttempts(0);
        factory.setCallTimeout(call);
        // The client pings once per check period and the broker drops a connection silent for a TTL,
        // so the TTL outlasts several pings.
        factory.setClientFailureCheckPeriod(call);
        factory.setConnectionTTL(call * 3);
        JmsPoolConnectionFactory pool = new JmsPoolConnectionFactory();
        pool.setConnectionFactory(factory);
        pool.setMaxConnections(1);
        pool.setMaxSessionsPerConnection(SESSIONS);
        return pool;
    }

    /** A Core failure in the SDK's terms. */
    static BrokerException failure(NodeHandle node, JMSException e) {
        if (e instanceof JMSSecurityException) {
            return Errors.credentialsRejected(node.id(), Plane.DATA, 401, Errors.CREDENTIALS_MESSAGE);
        }
        if (e instanceof InvalidSelectorException) {
            return Selectors.invalid("Invalid message filter.");
        }
        if (e instanceof InvalidDestinationException) {
            return new BrokerException(
                    Status.NOT_FOUND,
                    ErrorInfo.of(ErrorReasons.RESOURCE_GONE),
                    Dispatch.REJECTED,
                    node.id(),
                    "The destination is not on this node: " + e.getMessage());
        }
        if (hasCause(e, ActiveMQNotConnectedException.class)) {
            return Errors.unreachable(
                    node.id(), "Could not reach the broker over Core: " + e.getMessage(), Dispatch.NOT_SENT, e);
        }
        return Errors.unreachable(node.id(), "The Core call failed: " + e.getMessage(), Dispatch.IN_DOUBT, e);
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        pools.values().forEach(pool -> {
            pool.stop();
            if (pool.getConnectionFactory() instanceof ActiveMQConnectionFactory factory) {
                factory.close();
            }
        });
        pools.clear();
    }
}
