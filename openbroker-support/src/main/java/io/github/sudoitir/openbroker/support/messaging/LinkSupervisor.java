package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Recovery;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

/**
 * The life of one link: {@code OPEN}, {@code RECOVERING}, {@code FAILED} or {@code CLOSED}. The
 * driver reports a loss with {@link #lost}; the supervisor then reopens the link through {@link
 * Link#reopen} with the recovery's backoff until it works or {@code giveUpAfter} passes. Every
 * attempt is a data-plane call named {@code link.recover}, admitted by the connection's interceptor.
 * Listeners get the events one at a time and in order, on a virtual thread, never on the thread that
 * reported the loss.
 */
public final class LinkSupervisor implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(LinkSupervisor.class.getName());
    private static final String RECOVER = "link.recover";
    private static final int MAX_PENDING_EVENTS = 256;

    /** What a driver does for its link. */
    public interface Link {

        /**
         * Opens the link again, on its node or on the member of its HA group now serving, and gives
         * the engine objects the new session (a pipeline's {@link SendPipeline#resume}). Returns the
         * node that serves the link. One attempt: a failure is retried after the backoff.
         */
        NodeId reopen() throws Exception;

        /** The link was lost; called first on the recovery thread. */
        default void suspended(BrokerException cause) {}

        /** The recovery gave up; the link is {@code FAILED}. */
        default void failed(BrokerException cause) {}
    }

    private final String id;
    private final NodeId node;
    private final String endpoint;
    private final Recovery recovery;
    private final Link link;
    private final EngineContext context;
    private final CopyOnWriteArrayList<LinkListener> listeners = new CopyOnWriteArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<LinkEvent> events = new ArrayDeque<>();
    private LinkState state = LinkState.OPEN;
    private boolean draining;
    private @Nullable Thread recoveryThread;

    /**
     * @param id the link's name, as {@link LinkEvent#link()}
     * @param node the node the link belongs to
     * @param endpoint the address a reopen calls, for the interceptor
     */
    public LinkSupervisor(
            String id, NodeId node, String endpoint, Recovery recovery, Link link, EngineContext context) {
        this.id = Objects.requireNonNull(id, "id");
        this.node = Objects.requireNonNull(node, "node");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.link = Objects.requireNonNull(link, "link");
        this.context = Objects.requireNonNull(context, "context");
    }

    public LinkState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** Receives the events from now on; close the result to stop. */
    public AutoCloseable listen(LinkListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return () -> listeners.remove(listener);
    }

    /**
     * The link broke. The first call while {@code OPEN} starts the recovery; calls in any other state
     * do nothing. Cheap and non-blocking, so a client's I/O thread may call it.
     */
    public void lost(BrokerException cause) {
        lock.lock();
        try {
            if (state != LinkState.OPEN) {
                return;
            }
            state = LinkState.RECOVERING;
            emit(LinkState.RECOVERING, Optional.empty(), Optional.of(cause));
            recoveryThread = context.start("openbroker-recover-" + id, () -> recover(cause));
        } finally {
            lock.unlock();
        }
    }

    /** Ends the link and any recovery under way. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (state == LinkState.CLOSED) {
                return;
            }
            state = LinkState.CLOSED;
            emit(LinkState.CLOSED, Optional.empty(), Optional.empty());
            if (recoveryThread != null) {
                recoveryThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    private void recover(BrokerException cause) {
        try {
            link.suspended(cause);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "A link failed to suspend", e);
        }
        Optional<Instant> deadline =
                recovery.giveUpAfter().map(limit -> context.clock().instant().plus(limit));
        BrokerException last = cause;
        for (int attempt = 1; state() == LinkState.RECOVERING; attempt++) {
            if (expired(deadline)) {
                giveUp(last);
                return;
            }
            try {
                reopened(attempt());
                return;
            } catch (BrokerException e) {
                last = e;
            }
            Duration wait = Backoffs.delay(recovery.backoff(), attempt, context.random());
            if (deadline.isPresent()) {
                Duration remaining = Duration.between(context.clock().instant(), deadline.get());
                wait = wait.compareTo(remaining) > 0 ? remaining : wait;
            }
            try {
                if (wait.isPositive()) {
                    context.sleeper().sleep(wait);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private boolean expired(Optional<Instant> deadline) {
        return deadline.isPresent() && !context.clock().instant().isBefore(deadline.get());
    }

    private NodeId attempt() {
        CallInfo call = new CallInfo(node, endpoint, Plane.DATA, RECOVER, 1, Optional.empty());
        try (Permit permit = context.interceptor().admit(call)) {
            try {
                NodeId served = link.reopen();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return served;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                BrokerException failure =
                        Failures.unavailable(node, Dispatch.NOT_SENT, "The link could not be reopened", e);
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            }
        }
    }

    private void reopened(NodeId served) {
        lock.lock();
        try {
            if (state == LinkState.RECOVERING) {
                state = LinkState.OPEN;
                emit(LinkState.OPEN, Optional.of(served), Optional.empty());
            }
        } finally {
            lock.unlock();
        }
    }

    private void giveUp(BrokerException cause) {
        lock.lock();
        try {
            if (state != LinkState.RECOVERING) {
                return;
            }
            state = LinkState.FAILED;
            emit(LinkState.FAILED, Optional.empty(), Optional.of(cause));
        } finally {
            lock.unlock();
        }
        try {
            link.failed(cause);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "A link failed to record its failure", e);
        }
    }

    /** Queues an event; the caller holds the lock, which fixes the order. */
    private void emit(LinkState to, Optional<NodeId> servedBy, Optional<BrokerException> cause) {
        if (events.size() >= MAX_PENDING_EVENTS) {
            events.pollFirst();
        }
        events.addLast(
                new LinkEvent(id, node, to, servedBy, cause, context.clock().instant()));
        if (!draining) {
            draining = true;
            context.start("openbroker-events-" + id, this::drain);
        }
    }

    private void drain() {
        while (true) {
            LinkEvent event;
            lock.lock();
            try {
                event = events.pollFirst();
                if (event == null) {
                    draining = false;
                    return;
                }
            } finally {
                lock.unlock();
            }
            for (LinkListener listener : listeners) {
                try {
                    listener.changed(event);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "A link listener failed", e);
                }
            }
        }
    }
}
