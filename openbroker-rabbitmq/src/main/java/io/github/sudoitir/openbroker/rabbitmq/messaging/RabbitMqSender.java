package io.github.sudoitir.openbroker.rabbitmq.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.amqp.MessageCodec;
import io.github.sudoitir.openbroker.support.messaging.LinkSupervisor;
import io.github.sudoitir.openbroker.support.messaging.SendPipeline;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;

/**
 * {@link Sender} on the send pipeline: one confirm-mode channel per link, a {@code mandatory} publish, and confirms and
 * returns settled through the {@link ConfirmTracker}.
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: the channel is in confirm mode and the publish is {@code mandatory}. A {@code
 *       basic.ack} confirms it, a {@code basic.return} before the ack fails it {@code NOT_ROUTED}, a {@code
 *       basic.nack} fails it {@code DESTINATION_FULL}. When the channel is lost the supervisor reopens it and the
 *       pipeline writes the unconfirmed messages again, in order, marked resent.
 *   <li>{@code AT_MOST_ONCE}: no confirm mode; the send is done when it is written, and never written again.
 * </ul>
 *
 * A message is durable; one that says it is not is refused under {@code AT_LEAST_ONCE}. Deduplication is not offered.
 */
final class RabbitMqSender implements Sender {

    private static final Set<KindId> QUEUES =
            Set.of(RabbitMqKinds.CLASSIC_QUEUE, RabbitMqKinds.QUORUM_QUEUE, RabbitMqKinds.STREAM);
    private static final String DEFAULT_EXCHANGE = "amq.default";

    /** Where a sender publishes. */
    record Destination(String vhost, String exchange, String routingKey, ResourceId resource, boolean queue) {

        /** The destination a send target names. */
        static Destination of(SendTarget target) {
            return switch (target) {
                case SendTarget.ToDestination to -> {
                    if (to.partition().isPresent()) {
                        throw RabbitMqErrors.invalid("A RabbitMQ destination has no partitions.");
                    }
                    ResourceId resource = to.destination();
                    if (QUEUES.contains(resource.kind())) {
                        yield viaDefaultExchange(resource);
                    }
                    if (!RabbitMqKinds.EXCHANGE.equals(resource.kind())) {
                        throw RabbitMqErrors.invalid("A RabbitMQ message is sent to an exchange or a queue, not to "
                                + resource.kind() + ".");
                    }
                    List<String> segments = segments(resource);
                    String name = segments.get(1);
                    yield new Destination(
                            segments.get(0),
                            DEFAULT_EXCHANGE.equals(name) ? "" : name,
                            to.routingKey().orElse(""),
                            resource,
                            false);
                }
                case SendTarget.ToSubscription to -> {
                    if (!QUEUES.contains(to.subscription().kind())) {
                        throw RabbitMqErrors.invalid("A RabbitMQ subscription is a queue or a stream, not "
                                + to.subscription().kind() + ".");
                    }
                    yield viaDefaultExchange(to.subscription());
                }
            };
        }

        private static Destination viaDefaultExchange(ResourceId queue) {
            List<String> segments = segments(queue);
            return new Destination(segments.get(0), "", segments.get(1), queue, true);
        }

        private static List<String> segments(ResourceId resource) {
            List<String> segments = resource.path().segments();
            if (segments.size() != 2) {
                throw RabbitMqErrors.invalid(
                        "The id " + resource.format() + " is a virtual host and a name, such as /, orders.");
            }
            return segments;
        }
    }

    /** The channel of one generation of the link, with the tracker of its confirms. */
    private record Open(Channel channel, ConfirmTracker tracker, int generation, NodeId served) {}

    private final LinkEnv env;
    private final Destination destination;
    private final SenderOptions options;
    private final boolean confirms;
    private final String id = UUID.randomUUID().toString();
    private final SendPipeline pipeline;
    private final LinkSupervisor supervisor;
    private final Object reopening = new Object();
    private volatile Open current;
    private volatile boolean closed;

    /** A sender over connections of its own, for a caller that has no transport. */
    RabbitMqSender(RabbitMqCluster cluster, SendTarget target, SenderOptions options) {
        this(new LinkEnv(cluster), target, options);
    }

    RabbitMqSender(LinkEnv env, SendTarget target, SenderOptions options) {
        this.env = env;
        this.destination = Destination.of(target);
        this.options = options;
        this.confirms = options.guarantee() == Guarantee.AT_LEAST_ONCE;
        this.current = open(0);
        this.supervisor = new LinkSupervisor(
                id, RabbitMqCluster.CLUSTER, env.endpoint(), options.recovery(), new Recover(), env.engine());
        this.supervisor.listen(env::fire);
        try {
            this.pipeline = SendPipeline.open(
                    id,
                    RabbitMqCluster.CLUSTER,
                    options,
                    env.info(OperationType.SEND, destination.resource(), Optional.empty(), 1),
                    env.engine(),
                    this::write,
                    supervisor::lost);
        } catch (RuntimeException e) {
            supervisor.close();
            closeQuietly(current.channel());
            throw e;
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

    @Override
    public Optional<DeduplicationWindow> deduplication() {
        return Optional.empty();
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

    @Override
    public void commit() {
        throw LinkEnv.precondition("Only a TRANSACTED sender can commit, and RabbitMQ offers none.");
    }

    @Override
    public void rollback() {
        throw LinkEnv.precondition("Only a TRANSACTED sender can roll back, and RabbitMQ offers none.");
    }

    private void requireDurable(OutboundMessage message) {
        if (confirms && message.headers().durable().filter(durable -> !durable).isPresent()) {
            throw RabbitMqErrors.invalid("A message sent " + options.guarantee()
                    + " is durable; a non-durable one is refused. Leave durable unset, or send AT_MOST_ONCE.");
        }
    }

    // ---- the channel -------------------------------------------------------------------------

    /** Opens a channel for generation {@code generation}: checks the target, and puts the channel in confirm mode. */
    private Open open(int generation) {
        Deadline deadline = env.deadline();
        checkTarget(deadline);
        Connection connection = env.connections().connection(destination.vhost(), deadline);
        Channel channel = null;
        try {
            channel = connection.createChannel();
            ConfirmTracker tracker = new ConfirmTracker(Optional.of(destination.resource()));
            if (confirms) {
                channel.confirmSelect();
                channel.addConfirmListener(tracker::ack, tracker::nack);
                channel.addReturnListener(returned -> onReturn(tracker, returned));
            }
            channel.addShutdownListener(cause -> shutdown(generation, tracker, cause));
            return new Open(channel, tracker, generation, LinkEnv.served(connection));
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw LinkEnv.failure(e, Optional.of(destination.resource()));
        }
    }

    private void checkTarget(Deadline deadline) {
        if (destination.queue()) {
            env.calls().queueDeclarePassive(destination.vhost(), destination.routingKey(), deadline);
        } else if (!destination.exchange().isEmpty()) {
            env.calls().exchangeDeclarePassive(destination.vhost(), destination.exchange(), deadline);
        }
    }

    private static void onReturn(ConfirmTracker tracker, Return returned) {
        tracker.returned(
                key(returned.getExchange(), returned.getRoutingKey(), returned.getProperties()),
                returned.getReplyCode(),
                returned.getReplyText());
    }

    private static String key(String exchange, String routingKey, AMQP.BasicProperties properties) {
        String messageId = properties.getMessageId();
        return exchange + '\u0000' + routingKey + '\u0000' + (messageId == null ? "" : messageId);
    }

    /** The channel of {@code generation} closed: its unconfirmed publishes are in doubt, and the link is lost. */
    private void shutdown(int generation, ConfirmTracker tracker, ShutdownSignalException cause) {
        tracker.channelLost();
        lost(generation, cause);
    }

    private void lost(int generation, Throwable cause) {
        if (!closed && supervisor != null && generation == current.generation()) {
            supervisor.lost(LinkEnv.down("The channel", cause));
        }
    }

    // ---- the wire ----------------------------------------------------------------------------

    private void write(OutboundMessage message, SendPipeline.Confirmation confirmation) {
        Open link = current;
        AMQP.BasicProperties properties;
        byte[] body;
        try {
            properties =
                    MessageCodec.properties(message, env.cluster().options().clock());
            body = MessageCodec.body(message);
        } catch (RuntimeException e) {
            throw LinkEnv.failure(e, Optional.of(destination.resource()));
        }
        if (!link.channel().isOpen()) {
            IOException closedChannel = new IOException("The channel is closed");
            lost(link.generation(), closedChannel);
            throw LinkEnv.down("The channel", closedChannel);
        }
        SendReceipt receipt =
                new SendReceipt(message.headers().id(), Optional.empty(), Optional.of(link.served()), false);
        long sequence = -1;
        try {
            synchronized (link) {
                if (confirms) {
                    CompletableFuture<SendReceipt> pending = new CompletableFuture<>();
                    sequence = link.channel().getNextPublishSeqNo();
                    link.tracker()
                            .published(
                                    sequence,
                                    key(destination.exchange(), destination.routingKey(), properties),
                                    pending,
                                    receipt);
                    pending.whenComplete((done, failure) -> answered(done, failure, confirmation));
                }
                link.channel()
                        .basicPublish(destination.exchange(), destination.routingKey(), confirms, properties, body);
            }
        } catch (IOException | ShutdownSignalException e) {
            link.tracker().withdraw(sequence);
            lost(link.generation(), e);
            throw LinkEnv.down("The channel", e);
        }
        if (!confirms) {
            confirmation.confirmed(receipt);
        }
    }

    /** The answer for one publish: a lost channel is left to the recovery, which writes the message again. */
    private void answered(SendReceipt receipt, @Nullable Throwable failure, SendPipeline.Confirmation confirmation) {
        if (failure == null) {
            confirmation.confirmed(receipt);
        } else if (!ConfirmTracker.isLoss(failure)) {
            confirmation.failed(LinkEnv.failure(failure, Optional.of(destination.resource())));
        }
    }

    // ---- recovery ----------------------------------------------------------------------------

    private final class Recover implements LinkSupervisor.Link {

        @Override
        public void suspended(BrokerException cause) {
            current.tracker().channelLost();
            pipeline.suspend(cause);
        }

        @Override
        public NodeId reopen() {
            synchronized (reopening) {
                Open old = current;
                closeQuietly(old.channel());
                Open fresh = open(old.generation() + 1);
                current = fresh;
                pipeline.resume(RabbitMqSender.this::write);
                return fresh.served();
            }
        }

        @Override
        public void failed(BrokerException cause) {
            pipeline.fail(cause);
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
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            // The channel is gone either way.
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            pipeline.close();
        } finally {
            closed = true;
            supervisor.close();
            closeQuietly(current.channel());
            env.forget(this);
        }
    }
}
