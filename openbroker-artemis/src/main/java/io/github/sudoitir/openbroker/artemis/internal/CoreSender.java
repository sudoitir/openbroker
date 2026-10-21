package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.messaging.SendPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;

/**
 * A sender over a {@link SendPipeline}. The pipeline bounds what is unconfirmed and resends it after
 * a recovery; this class is its writer on a Core session.
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: the send is asynchronous and the receipt completes when the broker
 *       acknowledges it, once it holds the message as the journal does; a refusal completes it
 *       with the mapped error.
 *   <li>{@code AT_MOST_ONCE}: the receipt completes when the message is written; it is never sent
 *       again, and a loss fails it {@code UNAVAILABLE}, {@code IN_DOUBT}.
 *   <li>{@code TRANSACTED}: sends join the session's transaction and their receipts complete when
 *       they are written; {@link #commit} takes effect for all of them, and fails {@code ABORTED}
 *       if the link was lost since the first of them.
 * </ul>
 *
 * Messages are durable unless they say otherwise, and an explicit non-durable message is refused
 * under {@code AT_LEAST_ONCE} and {@code TRANSACTED}.
 */
final class CoreSender implements Sender, CoreSession.Hooks<ClientProducer> {

    private final TransportEnv env;
    private final NodeHandle node;
    private final ResourceId resource;
    private final SimpleString destination;
    private final SenderOptions options;
    private final boolean transacted;
    private final Optional<DeduplicationWindow> window;
    private final SpoolDirectory spool;
    private final String id = UUID.randomUUID().toString();
    private final SendPipeline pipeline;
    private final CoreSession<ClientProducer> core;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<CoreBodies.Built, Boolean> inFlight = new IdentityHashMap<>();
    private final List<CoreBodies.Built> batch = new ArrayList<>();
    private int batchGeneration = -1;
    private boolean closed;

    /**
     * Opens the sender's link on {@code node}.
     *
     * @param subscription the queue when the target is one, for telemetry
     * @param destination the Core address, or the fully qualified queue name
     */
    static CoreSender open(
            TransportEnv env,
            NodeHandle node,
            ResourceId resource,
            Optional<ResourceId> subscription,
            String destination,
            String address,
            SenderOptions options)
            throws ActiveMQException {
        CoreSender sender = new CoreSender(env, node, resource, subscription, destination, address, options);
        try {
            sender.core.open();
        } catch (ActiveMQException | RuntimeException e) {
            sender.discard();
            throw e;
        }
        return sender;
    }

    private CoreSender(
            TransportEnv env,
            NodeHandle node,
            ResourceId resource,
            Optional<ResourceId> subscription,
            String destination,
            String address,
            SenderOptions options) {
        this.env = env;
        this.node = node;
        this.resource = resource;
        this.destination = SimpleString.of(destination);
        this.options = options;
        this.transacted = options.guarantee() == Guarantee.TRANSACTED;
        this.window = Optional.of(env.addresses().of(node, address).window());
        this.spool = env.spool();
        OperationInfo info = env.info(OperationType.SEND, resource, subscription, node, 1);
        this.pipeline = SendPipeline.open(id, node.id(), options, info, env.engine(), this::write, cause -> {
            // The write that found the link down has already started its recovery.
        });
        this.core = new CoreSession<>(
                id,
                env,
                node,
                CoreLinks.LinkSettings.sender(options.guarantee()),
                transacted,
                Optional.of(resource),
                options.recovery(),
                ClientSession::createProducer,
                this);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : core.state();
    }

    @Override
    public Optional<DeduplicationWindow> deduplication() {
        return window;
    }

    @Override
    public SendReceipt send(OutboundMessage message) {
        requireDurable(message);
        return pipeline.send(message);
    }

    @Override
    public Offer offer(OutboundMessage message) {
        requireDurable(message);
        return pipeline.offer(message);
    }

    @Override
    public void flush(Duration timeout) {
        pipeline.flush(timeout);
    }

    private void requireDurable(OutboundMessage message) {
        if (options.guarantee() != Guarantee.AT_MOST_ONCE
                && message.headers().durable().filter(durable -> !durable).isPresent()) {
            throw Queries.invalid("A message sent " + options.guarantee()
                    + " is durable; a non-durable one is refused. Leave durable unset, or send AT_MOST_ONCE.");
        }
    }

    // ---- the wire -----------------------------------------------------------

    private void write(OutboundMessage message, SendPipeline.Confirmation confirmation) {
        CoreSession.Attached<ClientProducer> attached = core.require();
        CoreBodies.Built built;
        try {
            built = CoreBodies.write(
                    attached.session(), message, env.cluster().options().clock(), Optional.of(spool));
        } catch (ActiveMQException e) {
            throw fail(attached, e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            switch (options.guarantee()) {
                case AT_LEAST_ONCE -> {
                    track(built);
                    attached.value()
                            .send(destination, built.message(), new Acknowledged(attached, built, confirmation));
                }
                case AT_MOST_ONCE -> {
                    attached.value().send(destination, built.message());
                    done(built);
                    confirmation.confirmed(receipt(attached, built));
                }
                case TRANSACTED -> {
                    attached.value().send(destination, built.message());
                    inBatch(attached, built);
                    confirmation.confirmed(receipt(attached, built));
                }
            }
        } catch (ActiveMQException e) {
            done(built);
            throw fail(attached, e);
        }
    }

    /** The failure of a write: a lost link keeps the message for the recovery; a refusal ends its receipt. */
    private BrokerException fail(CoreSession.Attached<ClientProducer> attached, ActiveMQException e) {
        if (LinkErrors.linkLevel(e)) {
            BrokerException down = LinkErrors.down(node.id(), "The link was lost: " + e.getMessage(), e);
            core.lost(attached.generation(), down);
            return down;
        }
        return CoreLinks.failure(node.id(), e, Optional.of(resource));
    }

    private SendReceipt receipt(CoreSession.Attached<ClientProducer> attached, CoreBodies.Built built) {
        Object userId = built.message().getUserID();
        return new SendReceipt(
                userId == null ? Optional.empty() : Optional.of("ID:" + userId),
                Optional.empty(),
                Optional.of(attached.served().id()),
                false);
    }

    private void track(CoreBodies.Built built) {
        lock.lock();
        try {
            inFlight.put(built, Boolean.TRUE);
        } finally {
            lock.unlock();
        }
    }

    /** Releases what a message held open, once it is settled one way or the other. */
    private void done(CoreBodies.Built built) {
        lock.lock();
        try {
            inFlight.remove(built);
        } finally {
            lock.unlock();
        }
        close(built);
    }

    private static void close(CoreBodies.Built built) {
        try {
            built.close();
        } catch (IOException e) {
            // the spool directory goes with the sender
        }
    }

    private void inBatch(CoreSession.Attached<ClientProducer> attached, CoreBodies.Built built) {
        lock.lock();
        try {
            if (batchGeneration >= 0 && batchGeneration != attached.generation()) {
                // The link recovered inside the transaction: what came before is gone.
                batchGeneration = Integer.MIN_VALUE;
            } else if (batchGeneration == -1) {
                batchGeneration = attached.generation();
            }
            batch.add(built);
        } finally {
            lock.unlock();
        }
    }

    /** The broker's answer to one asynchronous send. Runs on the client's I/O thread: it must not block. */
    private final class Acknowledged implements SendAcknowledgementHandler {

        private final CoreSession.Attached<ClientProducer> attached;
        private final CoreBodies.Built built;
        private final SendPipeline.Confirmation confirmation;

        Acknowledged(
                CoreSession.Attached<ClientProducer> attached,
                CoreBodies.Built built,
                SendPipeline.Confirmation confirmation) {
            this.attached = attached;
            this.built = built;
            this.confirmation = confirmation;
        }

        @Override
        public void sendAcknowledged(Message message) {
            done(built);
            confirmation.confirmed(receipt(attached, built));
        }

        @Override
        public void sendFailed(Message message, Exception e) {
            done(built);
            if (e instanceof ActiveMQException a) {
                BrokerException failure = fail(attached, a);
                if (failure.dispatch() != Dispatch.NOT_SENT) {
                    confirmation.failed(failure);
                }
                return;
            }
            confirmation.failed(
                    CoreLinks.failure(node.id(), new ActiveMQException(e.getMessage()), Optional.of(resource)));
        }
    }

    // ---- recovery -----------------------------------------------------------

    @Override
    public void suspended(BrokerException cause) {
        pipeline.suspend(cause);
        List<CoreBodies.Built> open;
        lock.lock();
        try {
            // The messages the lost session held open are written again from the pipeline.
            open = new ArrayList<>(inFlight.keySet());
            inFlight.clear();
        } finally {
            lock.unlock();
        }
        open.forEach(CoreSender::close);
    }

    @Override
    public void reopened(CoreSession.Attached<ClientProducer> attached) {
        pipeline.resume(this::write);
    }

    @Override
    public void failed(BrokerException cause) {
        pipeline.fail(cause);
    }

    // ---- the transaction ----------------------------------------------------

    @Override
    public void commit() {
        requireTransacted("commit");
        pipeline.flush(env.cluster().options().timeouts().call());
        try (Permit permit = env.links().admit(node, "sender.commit", Optional.of(resource))) {
            try {
                CoreSession.Attached<ClientProducer> attached = core.require();
                if (spoiled(attached)) {
                    attached.session().rollback();
                    throw LinkErrors.rolledBack(node.id());
                }
                attached.session().commit();
                permit.complete(Outcome.succeeded(), Optional.empty());
            } catch (ActiveMQException e) {
                BrokerException failure = CoreLinks.failure(node.id(), e, Optional.of(resource));
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        } finally {
            endBatch();
        }
    }

    /** Whether the link was lost since the transaction's first send. */
    private boolean spoiled(CoreSession.Attached<ClientProducer> attached) {
        lock.lock();
        try {
            return batchGeneration == Integer.MIN_VALUE
                    || (batchGeneration >= 0 && batchGeneration != attached.generation());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rollback() {
        requireTransacted("roll back");
        pipeline.flush(env.cluster().options().timeouts().call());
        try {
            core.require().session().rollback();
        } catch (ActiveMQException e) {
            throw CoreLinks.failure(node.id(), e, Optional.of(resource));
        } finally {
            endBatch();
        }
    }

    private void endBatch() {
        List<CoreBodies.Built> ended;
        lock.lock();
        try {
            ended = new ArrayList<>(batch);
            batch.clear();
            batchGeneration = -1;
        } finally {
            lock.unlock();
        }
        ended.forEach(CoreSender::close);
    }

    private void requireTransacted(String what) {
        if (!transacted) {
            throw Queries.invalid("This sender delivers each message as it is sent; it has nothing to " + what + ".");
        }
    }

    // ---- close --------------------------------------------------------------

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            pipeline.close();
        } finally {
            discard();
        }
    }

    private void discard() {
        core.close();
        endBatch();
        List<CoreBodies.Built> open;
        lock.lock();
        try {
            open = new ArrayList<>(inFlight.keySet());
            inFlight.clear();
        } finally {
            lock.unlock();
        }
        open.forEach(CoreSender::close);
        spool.close();
        env.forget(this);
    }
}
