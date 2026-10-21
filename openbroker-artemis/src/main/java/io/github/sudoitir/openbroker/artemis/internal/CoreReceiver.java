package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.ReceiveMode;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.jspecify.annotations.Nullable;

/**
 * A receiver on one queue, over a Core session that the driver reopens after a loss.
 *
 * <p>How each guarantee settles:
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: {@code Accept} acknowledges the message alone. {@code Release}
 *       without a delay hands back every message the receiver holds unsettled, uncounted, or
 *       counted when the delivery failed, so the broker's {@code max-delivery-attempts} applies; a
 *       lane holds one message, so for it that is the one. {@code Release} with a delay sends a copy
 *       to the queue with the delay, then acknowledges the original. {@code Reject} sends a copy to
 *       the dead-letter target, then acknowledges the original; with no target it is dropped, as
 *       the broker does with no dead-letter address.
 *   <li>{@code AT_MOST_ONCE}: the broker counts a message acknowledged when it delivers it. {@code
 *       Accept} does nothing; releasing or rejecting is refused.
 *   <li>{@code TRANSACTED}: settlements take effect on {@link #commit}; a release or reject is
 *       refused, since the transaction decides.
 * </ul>
 *
 * A message handed out before the link recovered is settled with {@code DELIVERY_STALE}: the broker
 * delivers it again. A transaction the loss ended commits as {@code ABORTED}.
 */
final class CoreReceiver implements Receiver, CoreSession.Hooks<ClientConsumer> {

    private static final Duration NEXT = Duration.ofMillis(200);
    private static final int SPOILED = Integer.MIN_VALUE;
    private static final SimpleString ORIGINAL_ADDRESS = SimpleString.of("_AMQ_ORIG_ADDRESS");
    private static final SimpleString ORIGINAL_QUEUE = SimpleString.of("_AMQ_ORIG_QUEUE");
    private static final SimpleString ORIGINAL_MESSAGE_ID = SimpleString.of("_AMQ_ORIG_MESSAGE_ID");

    private final TransportEnv env;
    private final NodeHandle node;
    private final ResourceId subscription;
    private final String destination;
    private final String address;
    private final ReceiverOptions options;
    private final boolean transacted;
    private final boolean browse;
    private final boolean preAcknowledged;
    private final Supplier<Optional<Sender>> deadLetterTarget;
    private final SpoolDirectory spool;
    private final Map<String, Held> unsettled = new LinkedHashMap<>();
    private final String id;
    private final OperationInfo receiveInfo;
    private final OperationInfo settleInfo;
    private final CoreSession<ClientConsumer> core;
    private int txGeneration = -1;
    private volatile boolean closed;

    private record Held(
            ClientMessage message, int generation, @Nullable Path file) {}

    /**
     * Opens the receiver's link on {@code node}.
     *
     * @param destination the queue's fully qualified name
     * @param windowBytes the bytes the consumer may buffer ahead; 0 buffers none
     * @param deadLetterTarget where a rejected message goes, when the caller named one
     */
    static CoreReceiver open(
            TransportEnv env,
            String id,
            NodeHandle node,
            ResourceId subscription,
            String destination,
            String selector,
            ReceiverOptions options,
            long windowBytes,
            Supplier<Optional<Sender>> deadLetterTarget)
            throws ActiveMQException {
        CoreReceiver receiver = new CoreReceiver(
                env, id, node, subscription, destination, selector, options, windowBytes, deadLetterTarget);
        try {
            receiver.core.open();
        } catch (ActiveMQException | RuntimeException e) {
            receiver.discard();
            throw e;
        }
        return receiver;
    }

    private CoreReceiver(
            TransportEnv env,
            String id,
            NodeHandle node,
            ResourceId subscription,
            String destination,
            String selector,
            ReceiverOptions options,
            long windowBytes,
            Supplier<Optional<Sender>> deadLetterTarget) {
        this.env = env;
        this.id = id;
        this.node = node;
        this.subscription = subscription;
        this.destination = destination;
        this.address = Targets.addressOf(destination);
        this.options = options;
        this.transacted = options.guarantee() == Guarantee.TRANSACTED;
        this.browse = options.mode() == ReceiveMode.BROWSE;
        this.preAcknowledged = options.guarantee() == Guarantee.AT_MOST_ONCE && !browse;
        this.deadLetterTarget = deadLetterTarget;
        this.spool = env.spool();
        this.receiveInfo = env.info(OperationType.RECEIVE, subscription, Optional.of(subscription), node, 0);
        this.settleInfo = env.info(OperationType.SETTLE, subscription, Optional.of(subscription), node, 1);
        SimpleString queue = SimpleString.of(destination);
        SimpleString filter = selector.isEmpty() ? null : SimpleString.of(selector);
        this.core = new CoreSession<>(
                id,
                env,
                node,
                CoreLinks.LinkSettings.receiver(options.guarantee(), windowBytes),
                transacted,
                Optional.of(subscription),
                options.recovery(),
                session -> session.createConsumer(queue, filter, browse),
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

    // ---- receive ------------------------------------------------------------

    @Override
    public List<Message> receive(int max, Duration timeout) {
        if (closed) {
            throw LinkErrors.closed(node.id(), "receiver");
        }
        Optional<CoreSession.Attached<ClientConsumer>> serving = core.tryCurrent();
        if (serving.isEmpty()) {
            serving = core.awaitOpen(timeout);
            if (serving.isEmpty()) {
                if (core.state() == LinkState.FAILED) {
                    core.require();
                }
                return List.of();
            }
        }
        int room = preAcknowledged ? max : Math.min(max, options.flow().maxUnsettled() - unsettled.size());
        if (room <= 0) {
            return List.of();
        }
        MessagingObserver observer = env.cluster().options().observer();
        Observation observation = observer == MessagingObserver.none() ? null : observer.onReceive(receiveInfo);
        List<Message> out = new ArrayList<>();
        BrokerException failure = null;
        CoreSession.Attached<ClientConsumer> attached = serving.get();
        try {
            ClientMessage next = attached.value().receive(Math.max(1, timeout.toMillis()));
            while (next != null) {
                out.add(hold(attached, next));
                if (out.size() >= room) {
                    break;
                }
                next = attached.value().receive(NEXT.toMillis());
            }
        } catch (ActiveMQException e) {
            if (LinkErrors.linkLevel(e)) {
                core.lost(attached.generation(), CoreLinks.failure(node.id(), e, Optional.of(subscription)));
            } else {
                failure = CoreLinks.failure(node.id(), e, Optional.of(subscription));
            }
        } catch (ActiveMQInterruptedException e) {
            Thread.currentThread().interrupt();
            failure = Deadlines.cancelled(node.id(), new InterruptedException(e.getMessage()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (observation != null) {
            observation.received(out);
            observation.end(failure);
        }
        if (failure != null && out.isEmpty()) {
            throw failure;
        }
        return out;
    }

    private Message hold(CoreSession.Attached<ClientConsumer> attached, ClientMessage m)
            throws ActiveMQException, IOException {
        Path[] file = new Path[1];
        CoreBodies.Spool tracking = size -> file[0] = spool.file(size);
        Message read = CoreBodies.read(m, subscription, node.id(), Long.MAX_VALUE, Optional.of(tracking));
        Message message = Copies.withAttempt(read, m.getDeliveryCount());
        unsettled.put(message.ref().position(), new Held(m, attached.generation(), file[0]));
        if (transacted) {
            touch(attached.generation());
        }
        return message;
    }

    // ---- settle -------------------------------------------------------------

    @Override
    public void settle(Message message, Disposition disposition) {
        String position = message.ref().position();
        Held held = unsettled.get(position);
        if (held == null) {
            if (preAcknowledged && disposition instanceof Disposition.Accept) {
                return;
            }
            throw Queries.invalid("This receiver holds no unsettled message " + position + ".");
        }
        MessagingObserver observer = env.cluster().options().observer();
        Observation observation =
                observer == MessagingObserver.none() ? null : observer.onSettle(settleInfo, message, disposition);
        BrokerException failure = null;
        try {
            settle(held, message, disposition);
        } catch (BrokerException e) {
            failure = e;
            throw e;
        } finally {
            if (observation != null) {
                observation.end(failure);
            }
        }
    }

    private void settle(Held held, Message message, Disposition disposition) {
        String position = message.ref().position();
        CoreSession.Attached<ClientConsumer> attached = core.tryCurrent()
                .filter(a -> a.generation() == held.generation())
                .orElse(null);
        Settlements.Action action = Settlements.of(disposition, options.guarantee(), browse, attached == null);
        try {
            switch (action) {
                case FORGET -> drop(position);
                case STALE -> {
                    drop(position);
                    throw LinkErrors.stale(node.id());
                }
                case REFUSED -> throw refused(disposition);
                case ACKNOWLEDGE -> {
                    held.message().individualAcknowledge();
                    drop(position);
                }
                case ROLLBACK -> rollbackAll(attached, false);
                case ROLLBACK_COUNTED -> rollbackAll(attached, true);
                case COPY_AFTER_DELAY -> {
                    Disposition.Release release = (Disposition.Release) disposition;
                    delayed(attached, held, message, release.delay().orElseThrow(), release.deliveryFailed());
                    drop(position);
                }
                case DEAD_LETTER -> {
                    deadLetter(
                            attached,
                            held,
                            message,
                            ((Disposition.Reject) disposition).reason().orElse("rejected"));
                    drop(position);
                }
            }
        } catch (ActiveMQException e) {
            throw failure(attached, e);
        }
    }

    private BrokerException refused(Disposition disposition) {
        String what = disposition instanceof Disposition.Reject ? "reject a message" : "release a message";
        if (transacted) {
            return Queries.invalid(
                    "A transacted receiver cannot " + what + "; it settles its batch with commit or rollback.");
        }
        return Queries.invalid("An AT_MOST_ONCE receiver's messages are settled when they are delivered; "
                + "there is nothing to " + what.substring(0, what.indexOf(' ')) + ".");
    }

    private BrokerException failure(CoreSession.Attached<ClientConsumer> attached, ActiveMQException e) {
        BrokerException failure = CoreLinks.failure(node.id(), e, Optional.of(subscription));
        if (LinkErrors.linkLevel(e)) {
            core.lost(attached.generation(), failure);
        }
        return failure;
    }

    private void drop(String position) {
        Held held = unsettled.remove(position);
        if (held != null) {
            spool.delete(held.file());
        }
    }

    /** Hands back every unsettled message; counted, they move toward dead-lettering. */
    void rollbackAll(CoreSession.Attached<ClientConsumer> attached, boolean counted) throws ActiveMQException {
        attached.session().rollback(counted);
        clear();
    }

    private void clear() {
        unsettled.values().forEach(h -> spool.delete(h.file()));
        unsettled.clear();
    }

    /** A copy of the message that is delivered after {@code delay}, as the next attempt; then the original is acknowledged. */
    private void delayed(
            CoreSession.Attached<ClientConsumer> attached, Held held, Message message, Duration delay, boolean counted)
            throws ActiveMQException {
        OutboundMessage copy = Copies.delayed(
                message, delay, counted, env.cluster().options().clock().instant());
        send(attached, SimpleString.of(destination), copy, m -> {});
        held.message().individualAcknowledge();
    }

    /** Sends the message to the dead-letter target, then acknowledges the original. */
    private void deadLetter(CoreSession.Attached<ClientConsumer> attached, Held held, Message message, String reason)
            throws ActiveMQException {
        OutboundMessage copy = Copies.deadLetter(message, reason);
        Optional<Sender> target = deadLetterTarget.get();
        if (target.isPresent()) {
            target.get().send(copy);
        } else {
            Optional<String> dead = env.addresses().of(node, address).deadLetterAddress();
            if (dead.isPresent()) {
                send(attached, SimpleString.of(dead.get()), copy, m -> {
                    m.putStringProperty(ORIGINAL_ADDRESS, SimpleString.of(address));
                    m.putStringProperty(ORIGINAL_QUEUE, SimpleString.of(subscription.name()));
                    m.putLongProperty(ORIGINAL_MESSAGE_ID, held.message().getMessageID());
                });
            }
        }
        held.message().individualAcknowledge();
    }

    /** Sends a message on the receiver's own session and waits for the broker to hold it. */
    private void send(
            CoreSession.Attached<ClientConsumer> attached,
            SimpleString to,
            OutboundMessage copy,
            Consumer<ClientMessage> decorate)
            throws ActiveMQException {
        try (CoreBodies.Built built = CoreBodies.write(
                        attached.session(), copy, env.cluster().options().clock(), Optional.of(spool));
                ClientProducer producer = attached.session().createProducer()) {
            decorate.accept(built.message());
            producer.send(to, built.message());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---- transaction --------------------------------------------------------

    /** Notes the generation a transaction's first work was done on; work on another spoils it. */
    private void touch(int generation) {
        if (txGeneration == -1) {
            txGeneration = generation;
        } else if (txGeneration != generation) {
            txGeneration = SPOILED;
        }
    }

    @Override
    public void commit() {
        if (!transacted) {
            throw Queries.invalid("This receiver settles each message; it has no batch to commit.");
        }
        try (Permit permit = env.links().admit(node, "receiver.commit", Optional.of(subscription))) {
            try {
                Optional<CoreSession.Attached<ClientConsumer>> serving = core.tryCurrent();
                if (serving.isEmpty() && txGeneration != -1) {
                    throw LinkErrors.rolledBack(node.id());
                }
                CoreSession.Attached<ClientConsumer> attached = core.require();
                if (txGeneration != -1 && txGeneration != attached.generation()) {
                    attached.session().rollback();
                    throw LinkErrors.rolledBack(node.id());
                }
                attached.session().commit();
                permit.complete(Outcome.succeeded(), Optional.empty());
            } catch (ActiveMQException e) {
                BrokerException failure = CoreLinks.failure(node.id(), e, Optional.of(subscription));
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        } finally {
            endTransaction();
        }
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    /** Rolls the transaction back; {@code counted}, the messages it held count a delivery. */
    void rollback(boolean counted) {
        try {
            Optional<CoreSession.Attached<ClientConsumer>> serving = core.tryCurrent();
            if (serving.isPresent()) {
                serving.get().session().rollback(counted);
            }
        } catch (ActiveMQException e) {
            throw CoreLinks.failure(node.id(), e, Optional.of(subscription));
        } finally {
            endTransaction();
        }
    }

    private void endTransaction() {
        clear();
        txGeneration = -1;
        joinedBatch.forEach(CoreReceiver::close);
        joinedBatch.clear();
    }

    private static void close(CoreBodies.Built built) {
        try {
            built.close();
        } catch (IOException e) {
            // the spool directory goes with the receiver
        }
    }

    // ---- joined sender ------------------------------------------------------

    private final List<CoreBodies.Built> joinedBatch = new ArrayList<>();

    @Override
    public Sender sender(SendTarget target) {
        if (!transacted) {
            throw Queries.invalid("Only a TRANSACTED receiver has a transaction to join a sender to.");
        }
        Targets.Resolved resolved = Targets.resolve(
                env.cluster(), node, target, Deadlines.current(env.cluster().options()));
        return new Joined(resolved);
    }

    /** Sends on the receiver's session, in its transaction; committed and rolled back with it. */
    private final class Joined implements Sender {

        private final Targets.Resolved target;
        private final SimpleString to;
        private final String joinedId = UUID.randomUUID().toString();
        private @Nullable ClientProducer producer;
        private int producerGeneration = -1;

        Joined(Targets.Resolved target) {
            this.target = target;
            this.to = SimpleString.of(target.destination());
        }

        @Override
        public String id() {
            return joinedId;
        }

        @Override
        public LinkState state() {
            return CoreReceiver.this.state();
        }

        @Override
        public Optional<DeduplicationWindow> deduplication() {
            return Optional.of(env.addresses().of(node, target.address()).window());
        }

        @Override
        public SendReceipt send(OutboundMessage message) {
            if (message.headers().durable().filter(durable -> !durable).isPresent()) {
                throw Queries.invalid("A message sent in a transaction is durable; a non-durable one is refused.");
            }
            CoreSession.Attached<ClientConsumer> attached = core.require();
            try {
                if (producer == null || producerGeneration != attached.generation()) {
                    producer = attached.session().createProducer();
                    producerGeneration = attached.generation();
                }
                CoreBodies.Built built = CoreBodies.write(
                        attached.session(), message, env.cluster().options().clock(), Optional.of(spool));
                joinedBatch.add(built);
                producer.send(to, built.message());
                touch(attached.generation());
                Object userId = built.message().getUserID();
                return new SendReceipt(
                        userId == null ? Optional.empty() : Optional.of("ID:" + userId),
                        Optional.empty(),
                        Optional.of(attached.served().id()),
                        false);
            } catch (ActiveMQException e) {
                throw failure(attached, e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Offer offer(OutboundMessage message) {
            return new Offer.Accepted(CompletableFuture.completedFuture(send(message)));
        }

        @Override
        public void flush(Duration timeout) {
            // each send is written in the transaction before it returns
        }

        @Override
        public void commit() {
            throw Queries.invalid("A joined sender commits with its receiver.");
        }

        @Override
        public void rollback() {
            throw Queries.invalid("A joined sender rolls back with its receiver.");
        }

        @Override
        public void close() {
            // the producer lives on the receiver's session
        }
    }

    // ---- recovery and close -------------------------------------------------

    @Override
    public void suspended(BrokerException cause) {
        // What was handed out belongs to the lost session; settling it is DELIVERY_STALE.
    }

    @Override
    public void reopened(CoreSession.Attached<ClientConsumer> attached) {
        // The consumer was recreated with the session; reading goes on from it.
    }

    @Override
    public void failed(BrokerException cause) {
        // The link is FAILED; reads and settles report it.
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        discard();
    }

    private void discard() {
        // The broker hands back whatever the session holds unsettled.
        core.close();
        unsettled.clear();
        joinedBatch.forEach(CoreReceiver::close);
        joinedBatch.clear();
        spool.close();
        env.forget(this);
    }
}
