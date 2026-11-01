package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiveMode;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.amqp.PooledAmqpConnections;
import io.github.sudoitir.openbroker.support.messaging.Declared;
import io.github.sudoitir.openbroker.support.messaging.EngineContext;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import io.github.sudoitir.openbroker.support.messaging.Retries;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * {@link Transport} over AMQP 0-9-1: static {@code capabilities()}, senders, receivers and processors, and a link
 * supervisor per link.
 *
 * <p>What it does on RabbitMQ:
 *
 * <ul>
 *   <li>A sender publishes {@code mandatory} on a confirm-mode channel of its own ({@code AT_LEAST_ONCE}), or without
 *       confirms ({@code AT_MOST_ONCE}). Nothing deduplicates, and there are no transactions.
 *   <li>A receiver or processor consumes a classic or a quorum queue with manual acknowledgement. A quorum queue
 *       counts deliveries and dead-letters at its delivery limit, so the default retry is the system's; a classic
 *       queue counts nothing, so the default is a managed retry in place. A stream has no consuming receiver; it is
 *       browsed.
 *   <li>The client's own recovery is off: a lost channel or connection is reported to the link's supervisor, which
 *       reopens it with the recovery's backoff.
 * </ul>
 */
public final class RabbitMqTransport implements Transport, AutoCloseable {

    private static final String SYSTEM_NAME = "RabbitMQ";

    private static final TransportCapabilities CAPABILITIES = new TransportCapabilities(
            Set.of(Guarantee.AT_MOST_ONCE, Guarantee.AT_LEAST_ONCE),
            DeduplicationScope.NONE,
            Set.of(OrderingScope.NONE),
            Set.of(PrefetchBound.MESSAGES),
            Map.of(
                    RabbitMqKinds.CLASSIC_QUEUE,
                    new SubscriptionCapabilities(
                            Set.of(OrderingScope.NONE), SettlementScope.MESSAGE, false, SystemRetry.NONE, false),
                    RabbitMqKinds.QUORUM_QUEUE,
                    new SubscriptionCapabilities(
                            Set.of(OrderingScope.NONE),
                            SettlementScope.MESSAGE,
                            false,
                            new SystemRetry(false, true, true),
                            true)),
            true);

    private final RabbitMqCluster cluster;
    private final LinkEnv env;
    private final AmqpConnections connections;
    private final boolean ownsConnections;

    /** A transport over AMQP connections of its own, closed with it. */
    public RabbitMqTransport(RabbitMqCluster cluster) {
        this(cluster, new PooledAmqpConnections(cluster), true);
    }

    /** A transport over {@code connections}, which its caller closes. */
    public RabbitMqTransport(RabbitMqCluster cluster, AmqpConnections connections) {
        this(cluster, connections, false);
    }

    private RabbitMqTransport(RabbitMqCluster cluster, AmqpConnections connections, boolean owns) {
        this.cluster = cluster;
        this.connections = connections;
        this.ownsConnections = owns;
        this.env = new LinkEnv(cluster, connections, EngineContext.of(cluster.options()));
    }

    /** What RabbitMQ offers; fixed, and answered without any I/O. */
    @Override
    public TransportCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        Declared.require(
                SYSTEM_NAME,
                CAPABILITIES,
                options.guarantee(),
                options.deduplication(),
                options.ordering(),
                CAPABILITIES.orderings());
        ResourceId destination = RabbitMqSender.Destination.of(target).resource();
        return env.admitted("sender.open", destination, () -> env.track(new RabbitMqSender(env, target, options)));
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        if (options.mode() == ReceiveMode.BROWSE) {
            if (!RabbitMqKinds.STREAM.equals(subscription.kind())) {
                throw RabbitMqErrors.unimplemented(
                        "browse " + subscription.kind() + ": only a stream can be browsed without taking messages");
            }
            return env.admitted(
                    "receiver.open", subscription, () -> env.track(new StreamReader(cluster, subscription)));
        }
        RabbitMqReceiver.check(subscription, options);
        SubscriptionCapabilities kind = CAPABILITIES.subscription(subscription.kind());
        Declared.require(
                SYSTEM_NAME,
                CAPABILITIES,
                options.guarantee(),
                DeduplicationScope.NONE,
                OrderingScope.NONE,
                kind.orderings());
        return env.admitted(
                "receiver.open", subscription, () -> env.track(new RabbitMqReceiver(env, subscription, options)));
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions asked, MessageHandler handler) {
        RabbitMqReceiver.check(subscription, asked.receive());
        SubscriptionCapabilities kind = CAPABILITIES.subscription(subscription.kind());
        Retries.Resolved retry = Retries.resolve(asked.retry(), kind.retry(), asked.ordering());
        ProcessorOptions options = asked.withRetry(retry.policy());
        Declared.require(
                SYSTEM_NAME,
                CAPABILITIES,
                options.receive().guarantee(),
                DeduplicationScope.NONE,
                options.ordering(),
                kind.orderings());
        if (options.receive().guarantee() != Guarantee.AT_LEAST_ONCE) {
            throw RabbitMqErrors.unimplemented("open a processor with the guarantee "
                    + options.receive().guarantee() + ": it settles each message, so only AT_LEAST_ONCE");
        }
        return env.admitted("processor.open", subscription, () -> {
            String id = UUID.randomUUID().toString();
            DeadLetterSink.Senders deadLetters = new DeadLetterSink.Senders(env, options.retry());
            RabbitMqLanes lanes = new RabbitMqLanes(env, id, subscription, options, deadLetters);
            try {
                Processor engine = ProcessorEngine.start(
                        id,
                        options,
                        retry.inPlace(),
                        env.info(OperationType.PROCESS, subscription, Optional.of(subscription), 1),
                        env.engine(),
                        lanes,
                        handler);
                return env.track(new Running(env, engine, lanes, deadLetters));
            } catch (RuntimeException e) {
                lanes.close();
                deadLetters.close();
                throw e;
            }
        });
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        return env.watch(listener);
    }

    @Override
    public void close() {
        env.close();
        if (ownsConnections) {
            connections.close();
        }
    }

    /** A processor with the resources its lanes share, released when it stops. */
    private static final class Running implements Processor {

        private final LinkEnv env;
        private final Processor engine;
        private final RabbitMqLanes lanes;
        private final DeadLetterSink.Senders deadLetters;

        Running(LinkEnv env, Processor engine, RabbitMqLanes lanes, DeadLetterSink.Senders deadLetters) {
            this.env = env;
            this.engine = engine;
            this.lanes = lanes;
            this.deadLetters = deadLetters;
        }

        @Override
        public String id() {
            return engine.id();
        }

        @Override
        public LinkState state() {
            return engine.state();
        }

        @Override
        public RetryPolicy retry() {
            return engine.retry();
        }

        @Override
        public void stop(Duration grace) {
            try {
                engine.stop(grace);
            } finally {
                release();
            }
        }

        @Override
        public void close() {
            try {
                engine.close();
            } finally {
                release();
            }
        }

        private void release() {
            lanes.close();
            deadLetters.close();
            env.forget(this);
        }
    }
}
