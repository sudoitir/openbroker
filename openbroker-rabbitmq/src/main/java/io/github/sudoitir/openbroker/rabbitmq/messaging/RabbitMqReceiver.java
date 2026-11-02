package io.github.sudoitir.openbroker.rabbitmq.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.ReceiveMode;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.amqp.MessageCodec;
import io.github.sudoitir.openbroker.support.messaging.LinkSupervisor;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Pull {@link Receiver} on {@code basicConsume} with manual acknowledgement, {@code basicQos} from the flow control, a
 * local buffer, settlement, and a delayed release that is held and then rejected.
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: a message is acknowledged when accepted, requeued when released and rejected without
 *       requeue (so the queue's dead-letter exchange applies) when rejected. Unsettled messages go back to the queue
 *       when the receiver closes or the channel is lost.
 *   <li>{@code AT_MOST_ONCE}: the broker acknowledges on delivery; there is nothing to settle.
 * </ul>
 *
 * A message's position is {@code <generation>:<delivery tag>}. The generation counts the channels of the link: a tag
 * is only good on the channel that delivered it, so settling a message after the channel was lost is refused with
 * {@code DELIVERY_STALE}.
 */
final class RabbitMqReceiver implements Receiver {

    private static final int MAX_QOS = 65535;
    private static final Set<KindId> QUEUES = Set.of(RabbitMqKinds.CLASSIC_QUEUE, RabbitMqKinds.QUORUM_QUEUE);
    private static final String CHANNEL = "The channel";
    private static final System.Logger LOG = System.getLogger(RabbitMqReceiver.class.getName());

    /** The channel of one generation of the link. */
    private record Open(Channel channel, int generation, NodeId served) {}

    /** A delivery handed out and not yet settled. */
    private static final class Held {
        private final long tag;
        private final int generation;
        private volatile boolean settling;

        Held(long tag, int generation) {
            this.tag = tag;
            this.generation = generation;
        }
    }

    @FunctionalInterface
    private interface ChannelSettlement {
        void run(long tag) throws IOException;
    }

    private final LinkEnv env;
    private final ResourceId subscription;
    private final ReceiverOptions options;
    private final String vhost;
    private final String queue;
    private final boolean autoAck;
    private final String id = UUID.randomUUID().toString();
    private final AtomicInteger generation = new AtomicInteger();
    private final LinkedBlockingQueue<Message> buffer = new LinkedBlockingQueue<>();
    private final Map<String, Held> outstanding = new ConcurrentHashMap<>();
    private final Set<Thread> releases = ConcurrentHashMap.newKeySet();
    private final Object channelLock = new Object();
    private final OperationInfo receiveInfo;
    private final OperationInfo settleInfo;
    private final LinkSupervisor supervisor;
    private final AtomicReference<Open> current = new AtomicReference<>();
    private final AtomicReference<@Nullable BrokerException> failure = new AtomicReference<>();
    private volatile boolean closed;

    /** A receiver over connections of its own, for a caller that has no transport. */
    RabbitMqReceiver(RabbitMqCluster cluster, ResourceId subscription, ReceiverOptions options) {
        this(new LinkEnv(cluster), subscription, options);
    }

    RabbitMqReceiver(LinkEnv env, ResourceId subscription, ReceiverOptions options) {
        check(subscription, options);
        this.env = env;
        this.subscription = subscription;
        this.options = options;
        this.vhost = subscription.path().segments().get(0);
        this.queue = subscription.path().segments().get(1);
        this.autoAck = options.guarantee() == Guarantee.AT_MOST_ONCE;
        this.receiveInfo = env.info(OperationType.RECEIVE, subscription, Optional.of(subscription), 1);
        this.settleInfo = env.info(OperationType.SETTLE, subscription, Optional.of(subscription), 1);
        this.current.set(open(0));
        this.supervisor = new LinkSupervisor(
                id, RabbitMqCluster.CLUSTER, env.endpoint(), options.recovery(), new Recover(), env.engine());
        this.supervisor.listen(env::fire);
    }

    /**
     * Refuses what a consuming receiver cannot do, before any channel is opened.
     *
     * @throws BrokerException {@code UNIMPLEMENTED} for a stream, a browse, a filter or a production-time bound, and
     *     {@code INVALID_ARGUMENT} for an id that is no queue
     */
    static void check(ResourceId subscription, ReceiverOptions options) {
        if (RabbitMqKinds.STREAM.equals(subscription.kind())) {
            throw RabbitMqErrors.unimplemented("receive from a stream: a stream keeps its messages; browse it");
        }
        if (!QUEUES.contains(subscription.kind())
                || subscription.path().segments().size() != 2) {
            throw RabbitMqErrors.invalid("A RabbitMQ subscription is a queue such as rabbitmq.classic-queue:/,orders,"
                    + " not " + subscription.format() + ".");
        }
        if (options.mode() == ReceiveMode.BROWSE) {
            throw RabbitMqErrors.unimplemented("browse a queue: only a stream can be browsed without taking messages");
        }
        if (options.producedAtOrBefore().isPresent()) {
            throw RabbitMqErrors.unimplemented("bound a read by production time: a queue keeps no production time");
        }
        if (options.filter().isPresent()) {
            throw RabbitMqErrors.unimplemented("filter messages on the broker: RabbitMQ has no server-side filter");
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : supervisor.state();
    }

    // ---- the channel -------------------------------------------------------------------------

    private Open open(int number) {
        Connection connection = env.connections().connection(vhost, env.deadline());
        Channel channel = null;
        try {
            channel = connection.createChannel();
            channel.addShutdownListener(cause -> lost(number, cause));
            channel.basicQos(Math.min(options.flow().maxUnsettled(), MAX_QOS));
            channel.basicConsume(
                    queue,
                    autoAck,
                    "openbroker-" + id + "-" + number,
                    (tag, delivery) -> deliver(number, delivery),
                    tag -> lost(number, new IOException("The broker cancelled the consumer")));
            return new Open(channel, number, LinkEnv.served(connection));
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw LinkEnv.failure(e, Optional.of(subscription));
        }
    }

    private void deliver(int number, Delivery delivery) {
        if (number != generation.get() || closed) {
            return;
        }
        long tag = delivery.getEnvelope().getDeliveryTag();
        Message read;
        try {
            read = MessageCodec.message(
                    subscription, delivery.getEnvelope(), delivery.getProperties(), delivery.getBody());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "A delivery could not be read; rejecting it", e);
            refuse(number, tag);
            return;
        }
        String position = number + ":" + tag;
        Message message = new Message(
                new MessageRef(read.ref().source(), read.ref().shard(), position),
                read.headers(),
                read.properties(),
                read.body(),
                read.truncated(),
                read.extensions());
        if (!autoAck) {
            outstanding.put(position, new Held(tag, number));
        }
        buffer.add(message);
    }

    private void refuse(int number, long tag) {
        Open link = current.get();
        if (autoAck || link == null || link.generation() != number) {
            return;
        }
        try {
            synchronized (channelLock) {
                link.channel().basicReject(tag, false);
            }
        } catch (IOException | ShutdownSignalException e) {
            LOG.log(System.Logger.Level.DEBUG, "A delivery could not be rejected", e);
        }
    }

    private void lost(int number, Throwable cause) {
        if (!closed && supervisor != null && number == generation.get()) {
            supervisor.lost(LinkEnv.down(CHANNEL, cause));
        }
    }

    // ---- receiving ---------------------------------------------------------------------------

    @Override
    public List<Message> receive(int max, Duration timeout) {
        try {
            return receiveInterruptibly(max, timeout);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    /** {@link #receive} for a caller that stops on interruption, as a processor's lane does. */
    List<Message> receiveInterruptibly(int max, Duration timeout) throws InterruptedException {
        if (closed) {
            throw LinkEnv.precondition("The receiver is closed.");
        }
        MessagingObserver observer = env.cluster().options().observer();
        Observation observation = observer == MessagingObserver.none() ? null : observer.onReceive(receiveInfo);
        List<Message> out = new ArrayList<>();
        Message first = buffer.poll(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        if (first != null) {
            out.add(first);
            buffer.drainTo(out, Math.max(0, max - 1));
        }
        BrokerException lastFailure = failure.get();
        boolean failed = out.isEmpty() && lastFailure != null;
        if (observation != null) {
            observation.received(out);
            observation.end(failed ? lastFailure : null);
        }
        if (failed) {
            throw lastFailure;
        }
        return out;
    }

    // ---- settling ----------------------------------------------------------------------------

    @Override
    public void settle(Message message, Disposition disposition) {
        env.settling(settleInfo, message, disposition, () -> settleObserved(message, disposition));
    }

    private void settleObserved(Message message, Disposition disposition) {
        if (autoAck) {
            if (disposition instanceof Disposition.Accept) {
                return;
            }
            throw LinkEnv.precondition("An AT_MOST_ONCE receiver's messages are acknowledged when delivered; they"
                    + " cannot be released or rejected.");
        }
        String position = message.ref().position();
        Held held = outstanding.get(position);
        Open link = current.get();
        if (held == null || held.settling) {
            throw LinkEnv.stale("The message " + message.ref() + " was not delivered by this receiver, or is settled.");
        }
        if (held.generation != generation.get()
                || link.generation() != held.generation
                || !link.channel().isOpen()) {
            outstanding.remove(position, held);
            throw LinkEnv.stale("The channel that delivered the message " + message.ref()
                    + " was lost; RabbitMQ delivers it again, so it cannot be settled.");
        }
        switch (disposition) {
            case Disposition.Accept _ ->
                finish(position, held, tag -> link.channel().basicAck(tag, false));
            case Disposition.Reject _ ->
                finish(position, held, tag -> link.channel().basicReject(tag, false));
            case Disposition.Release r -> {
                Duration delay = r.delay().orElse(Duration.ZERO);
                if (delay.isZero() || delay.isNegative()) {
                    finish(position, held, tag -> link.channel().basicNack(tag, false, true));
                } else {
                    release(position, held, link, delay);
                }
            }
        }
    }

    private void finish(String position, Held held, ChannelSettlement settlement) {
        held.settling = true;
        try {
            synchronized (channelLock) {
                settlement.run(held.tag);
            }
            outstanding.remove(position, held);
        } catch (ShutdownSignalException e) {
            outstanding.remove(position, held);
            throw LinkEnv.stale("The channel closed before the message could be settled: " + e.getMessage());
        } catch (IOException e) {
            held.settling = false;
            throw LinkEnv.failure(e, Optional.of(subscription));
        }
    }

    /** Holds the delivery unacknowledged, so it counts against the credit, and requeues it when it is due. */
    private void release(String position, Held held, Open link, Duration delay) {
        held.settling = true;
        Thread[] waiting = new Thread[1];
        waiting[0] = env.engine().start("openbroker-release-" + id, () -> {
            try {
                env.engine().sleeper().sleep(delay);
                if (!closed && held.generation == generation.get()) {
                    synchronized (channelLock) {
                        link.channel().basicNack(held.tag, false, true);
                    }
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            } catch (IOException | ShutdownSignalException e) {
                LOG.log(System.Logger.Level.DEBUG, "A delayed release found the channel gone", e);
            } finally {
                outstanding.remove(position, held);
                releases.remove(Thread.currentThread());
            }
        });
        releases.add(waiting[0]);
    }

    @Override
    public Sender sender(SendTarget target) {
        throw LinkEnv.precondition("Only a TRANSACTED receiver has a transaction to join, and RabbitMQ offers none.");
    }

    @Override
    public void commit() {
        throw LinkEnv.precondition("Only a TRANSACTED receiver can commit, and RabbitMQ offers none.");
    }

    @Override
    public void rollback() {
        throw LinkEnv.precondition("Only a TRANSACTED receiver can roll back, and RabbitMQ offers none.");
    }

    // ---- recovery ----------------------------------------------------------------------------

    private final class Recover implements LinkSupervisor.Link {

        @Override
        public void suspended(BrokerException cause) {
            // What was delivered on the lost channel goes back to the queue: forget it here.
            buffer.clear();
            outstanding.clear();
        }

        @Override
        public NodeId reopen() {
            closeQuietly(current.get().channel());
            buffer.clear();
            outstanding.clear();
            Open fresh = open(generation.incrementAndGet());
            current.set(fresh);
            return fresh.served();
        }

        @Override
        public void failed(BrokerException cause) {
            failure.set(cause);
        }
    }

    private static void closeQuietly(@Nullable Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException | ShutdownSignalException _) {
            // The channel is gone either way; the broker requeues what it held.
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        releases.forEach(Thread::interrupt);
        supervisor.close();
        closeQuietly(current.get().channel());
        buffer.clear();
        outstanding.clear();
        env.forget(this);
    }
}
