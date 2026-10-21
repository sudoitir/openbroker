package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Recovery;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.messaging.LinkSupervisor;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.jspecify.annotations.Nullable;

/**
 * The Core session of one link and what lives on it (a producer, a consumer), kept alive across
 * losses. The driver owns recovery, not the Artemis client: the client does not reconnect, and a
 * {@link LinkSupervisor} reopens the link on its node, then on the other members of the node's HA
 * group, creating the session and its producer or consumer anew and handing them to the link's
 * {@link Hooks}. Each session has a generation, so a message handed out by an older one is known
 * to be stale.
 *
 * @param <T> what the link attaches to a session
 */
final class CoreSession<T> implements LinkSupervisor.Link, AutoCloseable {

    /** Creates what lives on a session. */
    @FunctionalInterface
    interface Attach<T> {
        T attach(ClientSession session) throws ActiveMQException;
    }

    /** What the owner of the link does as it is lost, reopened and given up. */
    interface Hooks<T> {

        /** The link was lost; called on the recovery thread before the first attempt. */
        void suspended(BrokerException cause);

        /** The link is open again on {@code attached}. */
        void reopened(Attached<T> attached);

        /** The recovery gave up. */
        void failed(BrokerException cause);
    }

    /** A session of the link with what was attached to it. */
    record Attached<T>(CoreLinks.Lease lease, T value, int generation, NodeHandle served) {

        ClientSession session() {
            return lease.session();
        }
    }

    private final String id;
    private final TransportEnv env;
    private final NodeHandle origin;
    private final CoreLinks.LinkSettings settings;
    private final boolean transacted;
    private final Optional<ResourceId> resource;
    private final Attach<T> attach;
    private final Hooks<T> hooks;
    private final LinkSupervisor supervisor;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private @Nullable Attached<T> current;
    private @Nullable Attached<T> dying;
    private NodeHandle lastServed;
    private int generations;
    private boolean closed;

    CoreSession(
            String id,
            TransportEnv env,
            NodeHandle origin,
            CoreLinks.LinkSettings settings,
            boolean transacted,
            Optional<ResourceId> resource,
            Recovery recovery,
            Attach<T> attach,
            Hooks<T> hooks) {
        this.id = id;
        this.env = env;
        this.origin = origin;
        this.settings = settings;
        this.transacted = transacted;
        this.resource = resource;
        this.attach = attach;
        this.hooks = hooks;
        this.lastServed = origin;
        this.supervisor = new LinkSupervisor(id, origin.id(), origin.core().orElse(""), recovery, this, env.engine());
        this.supervisor.listen(event -> {
            env.fire(event);
            if (event.state() == LinkState.OPEN) {
                lostWhileReopening();
            }
        });
    }

    String id() {
        return id;
    }

    LinkState state() {
        return supervisor.state();
    }

    /** The node the link was opened on. */
    NodeHandle origin() {
        return origin;
    }

    /** Opens the link's first session on its node. */
    void open() throws ActiveMQException {
        attachOn(origin, true);
    }

    /** The session now serving, or empty while the link recovers. */
    Optional<Attached<T>> tryCurrent() {
        lock.lock();
        try {
            return Optional.ofNullable(current);
        } finally {
            lock.unlock();
        }
    }

    /** The session now serving; unavailable and not sent while the link recovers. */
    Attached<T> require() {
        lock.lock();
        try {
            if (closed) {
                throw LinkErrors.closed(origin.id(), "link");
            }
            if (current == null) {
                throw LinkErrors.down(
                        origin.id(),
                        supervisor.state() == LinkState.FAILED
                                ? "The link could not be recovered."
                                : "The link is recovering.",
                        null);
            }
            return current;
        } finally {
            lock.unlock();
        }
    }

    /** Waits up to {@code timeout} for the link to be open; empty if it is not by then, or is closed. */
    Optional<Attached<T>> awaitOpen(Duration timeout) {
        long remaining = timeout.toNanos();
        lock.lock();
        try {
            while (current == null && !closed && remaining > 0) {
                remaining = changed.awaitNanos(remaining);
            }
            return Optional.ofNullable(current);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /** The generation of the session now serving, or -1. */
    int generation() {
        lock.lock();
        try {
            return current == null ? -1 : current.generation();
        } finally {
            lock.unlock();
        }
    }

    /** The link broke on the session of {@code generation}; ignored when it is not the one serving. */
    void lost(int generation, BrokerException cause) {
        lock.lock();
        try {
            if (current == null || current.generation() != generation) {
                return;
            }
            dying = current;
            current = null;
        } finally {
            lock.unlock();
        }
        supervisor.lost(cause);
    }

    /**
     * A loss that comes after the new session is attached and before the supervisor calls the link
     * open is ignored by the supervisor, which is still recovering. When it reports the link open, a
     * link without a session is such a loss, and starts the recovery again.
     */
    private void lostWhileReopening() {
        boolean dead;
        lock.lock();
        try {
            dead = current == null && !closed;
        } finally {
            lock.unlock();
        }
        if (dead) {
            supervisor.lost(LinkErrors.down(origin.id(), "The link was lost as it reopened.", null));
        }
    }

    private Attached<T> attachOn(NodeHandle node, boolean respectRest) throws ActiveMQException {
        CoreLinks.Lease lease = env.links().lease(node, settings, transacted, respectRest);
        Attached<T> attached;
        try {
            T value = attach.attach(lease.session());
            lock.lock();
            try {
                if (closed) {
                    throw LinkErrors.closed(origin.id(), "link");
                }
                attached = new Attached<>(lease, value, ++generations, node);
                current = attached;
                lastServed = node;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        } catch (ActiveMQException | RuntimeException e) {
            lease.close();
            throw e;
        }
        lease.session().addFailureListener(new Watch(attached.generation()));
        if (lease.session().isClosed()) {
            lost(attached.generation(), LinkErrors.down(node.id(), "The connection closed as the link opened.", null));
        }
        return attached;
    }

    @Override
    public NodeId reopen() {
        BrokerException last = null;
        List<NodeHandle> candidates = env.groups().candidates(origin, lastServed());
        for (NodeHandle node : candidates) {
            try {
                Attached<T> attached = attachOn(node, false);
                hooks.reopened(attached);
                return node.id();
            } catch (ActiveMQException e) {
                last = CoreLinks.failure(node.id(), e, resource);
            } catch (BrokerException e) {
                last = e;
            }
        }
        throw last != null ? last : LinkErrors.down(origin.id(), "No node of the link's group answers.", null);
    }

    private NodeHandle lastServed() {
        lock.lock();
        try {
            return lastServed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void suspended(BrokerException cause) {
        Attached<T> old;
        lock.lock();
        try {
            old = dying;
            dying = null;
            if (current != null) {
                old = current;
                current = null;
            }
        } finally {
            lock.unlock();
        }
        hooks.suspended(cause);
        if (old != null) {
            Attached<T> stale = old;
            // Closing a session on a broken connection can wait out the call timeout; recovery does not.
            env.engine().start("openbroker-close-" + id, stale.lease()::close);
        }
    }

    @Override
    public void failed(BrokerException cause) {
        hooks.failed(cause);
    }

    @Override
    public void close() {
        Attached<T> serving;
        Attached<T> stale;
        lock.lock();
        try {
            closed = true;
            serving = current;
            stale = dying;
            current = null;
            dying = null;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        supervisor.close();
        if (serving != null) {
            serving.lease().close();
        }
        if (stale != null) {
            stale.lease().close();
        }
    }

    /** Reports the connection of one session breaking. Runs on the client's I/O thread: it must not block. */
    private final class Watch implements SessionFailureListener {

        private final int generation;

        Watch(int generation) {
            this.generation = generation;
        }

        @Override
        public void connectionFailed(ActiveMQException exception, boolean failedOver) {
            lost(generation, CoreLinks.failure(origin.id(), exception, resource));
        }

        @Override
        public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
            connectionFailed(exception, failedOver);
        }

        @Override
        public void beforeReconnect(ActiveMQException exception) {
            // The driver does not use the client's reconnection.
        }
    }
}
