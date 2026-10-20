package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLException;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidFilterExpressionException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A connection's Core links: one session factory per node, TLS material and purpose, opened on first
 * use and replaced once it fails. Operator calls (browse, send) and the transport use separate
 * links, so a long-lived receiver never holds what a browse waits for; a receiver that wants no
 * prefetch gets a link of its own, since the window is the factory's.
 *
 * <ul>
 *   <li>{@code useTopologyForLoadBalancing=false}: the broker pushes its topology, whose connector
 *       hosts are often unresolvable from the client, and a call would wedge reconnecting to one.
 *   <li>One connect attempt and no reconnect: a node that is down fails the call, and the caller
 *       decides what to do.
 *   <li>A 64 KiB consumer window by default: a browse of one page never buffers more.
 * </ul>
 */
final class CoreLinks implements AutoCloseable {

    /** Bytes a consumer buffers ahead of reading, unless it asked for none. */
    static final int WINDOW_BYTES = 64 * 1024;

    /**
     * How long an endpoint that could not be connected to is left alone. Calls in that time fail at
     * once as not sent, so a caller with another way to the node takes it without waiting out a
     * connect each time.
     */
    static final Duration REST = Duration.ofSeconds(30);

    /** What a link is for. */
    enum Purpose {
        OPERATOR,
        TRANSPORT
    }

    /** What a Core call does with its session. */
    @FunctionalInterface
    interface Work<T> {
        T run(ClientSession session) throws ActiveMQException;
    }

    private record Link(ServerLocator locator, ClientSessionFactory factory) {

        boolean usable() {
            return !factory.isClosed()
                    && factory.getConnection() != null
                    && !factory.getConnection().isDestroyed();
        }

        void close() {
            factory.close();
            locator.close();
        }
    }

    private final Cluster cluster;
    private final Map<String, Link> links = new ConcurrentHashMap<>();
    private final Map<String, Instant> resting = new ConcurrentHashMap<>();

    CoreLinks(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Runs one operator call on a fresh session of the node, admitted by the caller's interceptor.
     * The session is closed after it.
     */
    <T> T call(NodeHandle node, String operation, Optional<ResourceId> target, Deadline deadline, Work<T> work) {
        if (deadline.expired(cluster.options().clock())) {
            throw Deadlines.exceeded(node.id(), Dispatch.NOT_SENT);
        }
        try (Permit permit = admit(node, operation, target)) {
            try (ClientSession session = session(node, Purpose.OPERATOR, false, true)) {
                T result = work.run(session);
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (ActiveMQException e) {
                BrokerException failure = failure(node.id(), e);
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        }
    }

    /** Asks the caller's interceptor to admit a Core control call: an open, a browse, a send, a commit. */
    Permit admit(NodeHandle node, String operation, Optional<ResourceId> target) {
        return cluster.options()
                .interceptor()
                .admit(new CallInfo(node.id(), endpoint(node), Plane.DATA, operation, 1, target));
    }

    /**
     * A started session on the node's link for {@code purpose}. Transacted, its sends and
     * acknowledgements take effect on commit; otherwise each takes effect at once.
     *
     * @param buffered whether consumers on it may receive ahead of reading
     */
    ClientSession session(NodeHandle node, Purpose purpose, boolean transacted, boolean buffered)
            throws ActiveMQException {
        String endpoint = endpoint(node);
        Instant now = cluster.options().clock().instant();
        Instant until = resting.get(endpoint);
        if (until != null && now.isBefore(until)) {
            throw new BrokerException(
                    Status.UNAVAILABLE,
                    ErrorInfo.of(ErrorReasons.UNREACHABLE),
                    Dispatch.NOT_SENT,
                    node.id(),
                    "The node's Core endpoint did not answer a moment ago; it is tried again from " + until + ".");
        }
        Optional<TlsMaterial> tls = cluster.options().tls().current();
        String key = endpoint + "|" + tls.map(TlsMaterial::id).orElse("plain") + "|" + purpose + "|" + buffered;
        Link link = links.compute(key, (k, existing) -> {
            if (existing != null && existing.usable()) {
                return existing;
            }
            if (existing != null) {
                existing.close();
            }
            return connect(node, endpoint, tls, buffered);
        });
        Credentials account = cluster.credentials().forPlane(Plane.DATA);
        ClientSession session;
        try {
            session = link.factory()
                    .createSession(
                            account.isNone() ? null : account.username(),
                            account.isNone() ? null : account.password().reveal(),
                            false,
                            !transacted,
                            !transacted,
                            false,
                            0);
        } catch (ActiveMQSecurityException e) {
            throw Errors.credentialsRejected(node.id(), Plane.DATA, 401, Errors.CREDENTIALS_MESSAGE);
        }
        try {
            session.start();
            return session;
        } catch (ActiveMQException | RuntimeException e) {
            try {
                session.close();
            } catch (ActiveMQException closing) {
                // teardown; the failure that got here is the one to report
            }
            throw e;
        }
    }

    private Link connect(NodeHandle node, String endpoint, Optional<TlsMaterial> tls, boolean buffered) {
        ActiveMQConnectionFactory jms = connectionFactory(cluster.options(), endpoint, tls);
        ServerLocator locator = jms.getServerLocator();
        if (!buffered) {
            locator.setConsumerWindowSize(0);
        }
        try {
            Link link = new Link(locator, locator.createSessionFactory());
            resting.remove(endpoint);
            return link;
        } catch (Exception e) {
            locator.close();
            resting.put(endpoint, cluster.options().clock().instant().plus(REST));
            throw e instanceof ActiveMQException a
                    ? failure(node.id(), a)
                    : Errors.unreachable(
                            node.id(), "Could not open a Core connection: " + e.getMessage(), Dispatch.NOT_SENT, e);
        }
    }

    /** A Core connection factory for one endpoint, with the caller's timeouts and TLS material. */
    static ActiveMQConnectionFactory connectionFactory(
            ConnectOptions options, String endpoint, Optional<TlsMaterial> tls) {
        StringBuilder url = new StringBuilder(dialable(endpoint))
                .append(endpoint.contains("?") ? ";" : "?")
                .append("useTopologyForLoadBalancing=false;consumerWindowSize=")
                .append(WINDOW_BYTES)
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
        return factory;
    }

    /** A broker-advertised connector is often a bare {@code host:port}, which the client cannot dial. */
    static String dialable(String endpoint) {
        return endpoint.contains("://") ? endpoint : "tcp://" + endpoint;
    }

    private static String endpoint(NodeHandle node) {
        return node.core()
                .orElseThrow(() -> new BrokerException(
                        Status.FAILED_PRECONDITION,
                        ErrorInfo.of(ErrorReasons.ENDPOINT_NOT_FOUND),
                        Dispatch.NOT_SENT,
                        node.id(),
                        "This node has no Core endpoint."));
    }

    /** A Core failure in the SDK's terms. */
    static BrokerException failure(NodeId node, ActiveMQException e) {
        return switch (e) {
            case ActiveMQSecurityException s ->
                new BrokerException(
                        Status.PERMISSION_DENIED,
                        ErrorInfo.of(ErrorReasons.NOT_AUTHORIZED, Map.of(ErrorReasons.PLANE, Plane.DATA.name())),
                        Dispatch.REJECTED,
                        node,
                        "The broker refused this for the account: " + s.getMessage());
            case ActiveMQInvalidFilterExpressionException f -> Selectors.invalid("Invalid message filter.");
            case ActiveMQNonExistentQueueException q ->
                new BrokerException(
                        Status.NOT_FOUND,
                        ErrorInfo.of(ErrorReasons.RESOURCE_GONE),
                        Dispatch.REJECTED,
                        node,
                        "The queue is not on this node: " + q.getMessage());
            case ActiveMQQueueMaxConsumerLimitReached m ->
                new BrokerException(
                        Status.RESOURCE_EXHAUSTED,
                        ErrorInfo.of(ErrorReasons.MAX_CONSUMERS_REACHED),
                        Dispatch.REJECTED,
                        node,
                        "The queue already has as many consumers as it admits: " + m.getMessage());
            case ActiveMQDuplicateIdException d ->
                new BrokerException(
                        Status.ALREADY_EXISTS,
                        ErrorInfo.of(ErrorReasons.DUPLICATE_REJECTED),
                        Dispatch.REJECTED,
                        node,
                        "The broker has seen a duplicate id of this batch before and delivered none of it.");
            case ActiveMQAddressFullException full ->
                new BrokerException(
                        Status.RESOURCE_EXHAUSTED,
                        ErrorInfo.of(ErrorReasons.DESTINATION_FULL),
                        Dispatch.REJECTED,
                        node,
                        "The target address is full: " + full.getMessage());
            case ActiveMQTransactionOutcomeUnknownException u ->
                Errors.unreachable(node, "The commit's outcome is unknown: " + u.getMessage(), Dispatch.IN_DOUBT, u);
            case ActiveMQTransactionRolledBackException r ->
                new BrokerException(
                        Status.ABORTED,
                        ErrorInfo.of(ErrorReasons.BAD_RESPONSE),
                        Dispatch.REJECTED,
                        node,
                        "The broker rolled the transaction back: " + r.getMessage());
            case ActiveMQConnectionTimedOutException t ->
                Errors.unreachable(node, "The broker did not answer in time: " + t.getMessage(), Dispatch.IN_DOUBT, t);
            case ActiveMQNotConnectedException n ->
                cause(n, SSLException.class)
                        ? Errors.tls(node, Errors.TLS_MESSAGE, n)
                        : Errors.unreachable(
                                node, "Could not reach the broker over Core: " + n.getMessage(), Dispatch.NOT_SENT, n);
            default ->
                cause(e, ConnectException.class) || cause(e, UnknownHostException.class)
                        ? Errors.unreachable(
                                node, "Could not reach the broker over Core: " + e.getMessage(), Dispatch.NOT_SENT, e)
                        : Errors.unreachable(node, "The Core call failed: " + e.getMessage(), Dispatch.IN_DOUBT, e);
        };
    }

    private static boolean cause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        links.values().forEach(Link::close);
        links.clear();
    }
}
