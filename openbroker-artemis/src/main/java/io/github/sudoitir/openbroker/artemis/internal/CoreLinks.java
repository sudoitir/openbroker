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
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
 * A connection's Core links: one session factory per node, TLS material and {@link LinkSettings},
 * opened on first use and replaced once it fails or its TLS material is rotated. Operator calls
 * (browse, send) and the transport use separate links, so a long-lived receiver never holds what a
 * browse waits for; a link's consumer window, confirmation window and acknowledgement mode are the
 * factory's, so they are part of what tells two links apart.
 *
 * <ul>
 *   <li>{@code useTopologyForLoadBalancing=false}: the broker pushes its topology, whose connector
 *       hosts are often unresolvable from the client, and a call would wedge reconnecting to one.
 *   <li>One connect attempt and no reconnect: a node that is down fails the call, and the caller
 *       decides what to do. The driver's own recovery reopens the link.
 *   <li>A factory connects outside any map lock, under a lock of its key, so a slow connect holds
 *       back only callers of the same link.
 *   <li>A factory is reference-counted by the sessions open on it. Rotated TLS material gets a new
 *       factory for new sessions; the replaced one keeps serving the sessions it has and closes with
 *       the last of them, so rotation never ends a live link.
 * </ul>
 */
final class CoreLinks implements AutoCloseable {

    /** Bytes an operator consumer buffers ahead of reading: a browse of one page never buffers more. */
    static final int WINDOW_BYTES = 64 * 1024;

    /** Bytes of sends the broker may leave unacknowledged on an {@code AT_LEAST_ONCE} sender. */
    static final int CONFIRMATION_WINDOW_BYTES = 1024 * 1024;

    /**
     * How long an endpoint that could not be connected to is left alone. Calls in that time fail at
     * once as not sent, so a caller with another way to the node takes it without waiting out a
     * connect each time. A recovery attempt ignores it.
     */
    static final Duration REST = Duration.ofSeconds(30);

    /** What a link is for. */
    enum Purpose {
        OPERATOR,
        TRANSPORT
    }

    /**
     * What a link's factory is configured with. Two links with different settings never share a
     * factory.
     *
     * @param consumerWindow bytes a consumer buffers ahead; 0 buffers none
     * @param confirmationWindow bytes of sends the broker may leave unacknowledged; -1 asks for no
     *     acknowledgements
     * @param blockingSends whether a send waits for the broker's answer
     * @param blockingAcknowledgements whether an acknowledgement waits for the broker's answer
     * @param preAcknowledge whether the broker counts a message as acknowledged when it delivers it
     */
    record LinkSettings(
            Purpose purpose,
            int consumerWindow,
            int confirmationWindow,
            boolean blockingSends,
            boolean blockingAcknowledgements,
            boolean preAcknowledge) {

        /** An operator call; {@code buffered} lets its consumers read ahead. */
        static LinkSettings operator(boolean buffered) {
            return new LinkSettings(Purpose.OPERATOR, buffered ? WINDOW_BYTES : 0, -1, true, true, false);
        }

        /**
         * A sender: sends are not waited for one by one. {@code AT_LEAST_ONCE} asks the broker to
         * acknowledge each once it holds it; {@code AT_MOST_ONCE} asks for nothing; {@code TRANSACTED}
         * is answered at commit.
         */
        static LinkSettings sender(Guarantee guarantee) {
            int confirmation = guarantee == Guarantee.AT_LEAST_ONCE ? CONFIRMATION_WINDOW_BYTES : -1;
            return new LinkSettings(Purpose.TRANSPORT, 0, confirmation, false, true, false);
        }

        /**
         * A receiver: acknowledgements are not waited for, since a lost one is a duplicate, which
         * the guarantee allows; sends on its session (a copy for a delay, a dead letter or a joined
         * sender) are, since they are confirmed before the original is acknowledged.
         */
        static LinkSettings receiver(Guarantee guarantee, long prefetchBytes) {
            int window = (int) Math.min(prefetchBytes, Integer.MAX_VALUE);
            return new LinkSettings(
                    Purpose.TRANSPORT,
                    window,
                    -1,
                    true,
                    guarantee == Guarantee.TRANSACTED,
                    guarantee == Guarantee.AT_MOST_ONCE);
        }
    }

    /** What a Core call does with its session. */
    @FunctionalInterface
    interface Work<T> {
        T run(ClientSession session) throws ActiveMQException;
    }

    /** A session of a link; closing it releases the link's factory as well. */
    static final class Lease implements AutoCloseable {

        private final ClientSession session;
        private final Link link;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(ClientSession session, Link link) {
            this.session = session;
            this.link = link;
        }

        ClientSession session() {
            return session;
        }

        /** Closes the session, then lets go of the factory. */
        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                try {
                    session.close();
                } catch (ActiveMQException | RuntimeException e) {
                    // teardown; the connection may already be gone
                } finally {
                    link.release();
                }
            }
        }
    }

    /** One factory, counted by its open sessions. */
    private final class Link {

        private final ServerLocator locator;
        private final ClientSessionFactory factory;
        private final Optional<TlsMaterial> tls;
        private final ReentrantLock guard = new ReentrantLock();
        private int leases;
        private boolean retired;
        private boolean closed;

        Link(ServerLocator locator, ClientSessionFactory factory, Optional<TlsMaterial> tls) {
            this.locator = locator;
            this.factory = factory;
            this.tls = tls;
        }

        String tlsId() {
            return tls.map(TlsMaterial::id).orElse(PLAIN);
        }

        boolean usable() {
            return !factory.isClosed()
                    && factory.getConnection() != null
                    && !factory.getConnection().isDestroyed();
        }

        /** Takes a lease unless the factory is closed or broken. */
        boolean acquire() {
            guard.lock();
            try {
                if (closed || !usable()) {
                    return false;
                }
                leases++;
                return true;
            } finally {
                guard.unlock();
            }
        }

        void release() {
            guard.lock();
            try {
                leases--;
                if (retired && leases == 0) {
                    shut();
                }
            } finally {
                guard.unlock();
            }
        }

        /** No new sessions come to this factory; it closes with its last session. */
        void retire() {
            guard.lock();
            try {
                retired = true;
                if (leases == 0) {
                    shut();
                }
            } finally {
                guard.unlock();
            }
        }

        /** Closes the factory whatever it still serves. */
        void shutdown() {
            guard.lock();
            try {
                retired = true;
                shut();
            } finally {
                guard.unlock();
            }
        }

        private void shut() {
            if (closed) {
                return;
            }
            closed = true;
            open.remove(this);
            factory.close();
            locator.close();
            tls.ifPresent(CoreLinks.this::unuse);
        }
    }

    private static final String PLAIN = "plain";

    private final Cluster cluster;
    private final Map<String, Link> links = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> connecting = new ConcurrentHashMap<>();
    private final Set<Link> open = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> tlsUse = new HashMap<>();
    private final ReentrantLock tlsGuard = new ReentrantLock();
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
            try (Lease lease = lease(node, LinkSettings.operator(true), false, true)) {
                T result = work.run(lease.session());
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
     * A started session on the node's link for {@code settings}. Transacted, its sends and
     * acknowledgements take effect on commit; otherwise each takes effect at once. Close the lease
     * to close the session. The TLS material and credentials are asked for again at every call, so a
     * rotation reaches the next session and leaves the open ones alone.
     *
     * @param respectRest whether an endpoint that failed a moment ago is left alone; a recovery
     *     attempt is not
     */
    Lease lease(NodeHandle node, LinkSettings settings, boolean transacted, boolean respectRest)
            throws ActiveMQException {
        String endpoint = endpoint(node);
        Instant now = cluster.options().clock().instant();
        Instant until = respectRest ? resting.get(endpoint) : null;
        if (until != null && now.isBefore(until)) {
            throw new BrokerException(
                    Status.UNAVAILABLE,
                    ErrorInfo.of(ErrorReasons.UNREACHABLE),
                    Dispatch.NOT_SENT,
                    node.id(),
                    "The node's Core endpoint did not answer a moment ago; it is tried again from " + until + ".");
        }
        Link link = link(node, endpoint, cluster.options().tls().current(), settings);
        Credentials account = cluster.credentials().forPlane(Plane.DATA);
        ClientSession session;
        try {
            session = link.factory.createSession(
                    account.isNone() ? null : account.username(),
                    account.isNone() ? null : account.password().reveal(),
                    false,
                    !transacted,
                    !transacted,
                    settings.preAcknowledge(),
                    0);
        } catch (ActiveMQSecurityException e) {
            link.release();
            throw Errors.credentialsRejected(node.id(), Plane.DATA, 401, Errors.CREDENTIALS_MESSAGE);
        } catch (ActiveMQException | RuntimeException e) {
            link.release();
            throw e;
        }
        Lease lease = new Lease(session, link);
        try {
            session.start();
            return lease;
        } catch (ActiveMQException | RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    /** The factory for the settings and current TLS material, with a lease taken on it. */
    private Link link(NodeHandle node, String endpoint, Optional<TlsMaterial> tls, LinkSettings settings) {
        String key = endpoint + '|' + settings;
        String tlsId = tls.map(TlsMaterial::id).orElse(PLAIN);
        Link existing = links.get(key);
        if (existing != null && existing.tlsId().equals(tlsId) && existing.acquire()) {
            return existing;
        }
        ReentrantLock lock = connecting.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            existing = links.get(key);
            if (existing != null && existing.tlsId().equals(tlsId) && existing.acquire()) {
                return existing;
            }
            Link fresh = connect(node, endpoint, tls, settings);
            fresh.acquire();
            links.put(key, fresh);
            if (existing != null) {
                existing.retire();
            }
            return fresh;
        } finally {
            lock.unlock();
        }
    }

    private Link connect(NodeHandle node, String endpoint, Optional<TlsMaterial> tls, LinkSettings settings) {
        ActiveMQConnectionFactory jms = connectionFactory(cluster.options(), endpoint, tls, settings);
        ServerLocator locator = jms.getServerLocator();
        try {
            Link link = new Link(locator, locator.createSessionFactory(), tls);
            tls.ifPresent(this::use);
            open.add(link);
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

    private void use(TlsMaterial material) {
        tlsGuard.lock();
        try {
            tlsUse.merge(material.id(), 1, Integer::sum);
        } finally {
            tlsGuard.unlock();
        }
    }

    /** The last factory built from the material is gone: its context is not kept alive any longer. */
    private void unuse(TlsMaterial material) {
        tlsGuard.lock();
        try {
            if (tlsUse.merge(material.id(), -1, Integer::sum) <= 0) {
                tlsUse.remove(material.id());
                CoreTls.forget(material);
            }
        } finally {
            tlsGuard.unlock();
        }
    }

    /** A Core connection factory for one endpoint, with the caller's timeouts, TLS material and link settings. */
    static ActiveMQConnectionFactory connectionFactory(
            ConnectOptions options, String endpoint, Optional<TlsMaterial> tls, LinkSettings settings) {
        StringBuilder url = new StringBuilder(dialable(endpoint))
                .append(endpoint.contains("?") ? ";" : "?")
                .append("useTopologyForLoadBalancing=false;consumerWindowSize=")
                .append(settings.consumerWindow())
                .append(";confirmationWindowSize=")
                .append(settings.confirmationWindow())
                .append(";blockOnDurableSend=")
                .append(settings.blockingSends())
                .append(";blockOnNonDurableSend=")
                .append(settings.blockingSends())
                .append(";blockOnAcknowledge=")
                .append(settings.blockingAcknowledgements())
                .append(";preAcknowledge=")
                .append(settings.preAcknowledge())
                .append(';')
                .append(TransportConstants.NETTY_CONNECT_TIMEOUT)
                .append('=')
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
        return failure(node, e, Optional.empty());
    }

    /**
     * A Core failure in the SDK's terms. A refusal names the plane it came from and, when known, the
     * resource it was about.
     */
    static BrokerException failure(NodeId node, ActiveMQException e, Optional<ResourceId> resource) {
        return switch (e) {
            case ActiveMQSecurityException s -> {
                Map<String, String> metadata = new LinkedHashMap<>();
                metadata.put(ErrorReasons.PLANE, Plane.DATA.name());
                resource.ifPresent(id -> metadata.put(ErrorReasons.RESOURCE, id.format()));
                yield new BrokerException(
                        Status.PERMISSION_DENIED,
                        ErrorInfo.of(ErrorReasons.NOT_AUTHORIZED, metadata),
                        Dispatch.REJECTED,
                        node,
                        "The broker refused this for the account: " + s.getMessage());
            }
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
                        ErrorInfo.of(ErrorReasons.TRANSACTION_ROLLED_BACK),
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
        new ArrayList<>(open).forEach(Link::shutdown);
        links.clear();
    }
}
