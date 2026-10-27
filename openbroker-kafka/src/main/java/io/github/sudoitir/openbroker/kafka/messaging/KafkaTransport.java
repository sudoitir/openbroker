package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaErrors;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkListener;
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
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.support.messaging.Declared;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import io.github.sudoitir.openbroker.support.messaging.Retries;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The messaging surface of a Kafka connection: senders on producers, and receivers and processors
 * as members of consumer groups. Its client settings come from {@link KafkaCluster#clientConfig}.
 *
 * <p>What it does on Kafka:
 *
 * <ul>
 *   <li>Sending is idempotent within the producer's session ({@code SESSION} deduplication); a
 *       caller-set deduplication id is not kept across sessions, so a {@code WINDOW} ask is refused.
 *   <li>A consumer group's subscription settles each record on its own and commits the position of
 *       the lowest unsettled one. It has no retry of its own: the default policy is managed, in place,
 *       and an explicit managed policy on an unordered processor releases through retry topics.
 *   <li>Ordering of the whole subscription needs a one-partition topic.
 * </ul>
 */
public final class KafkaTransport implements Transport, AutoCloseable {

    private final KafkaCluster cluster;
    private final MessagingEnv env;
    private final Topics topics;
    private volatile TransportCapabilities capabilities;

    public KafkaTransport(KafkaCluster cluster) {
        this.cluster = cluster;
        this.env = new MessagingEnv(cluster);
        this.topics = new Topics(cluster);
    }

    /**
     * What Kafka offers, computed from the cluster's features the first time it is asked for and then
     * fixed for the connection.
     */
    @Override
    public TransportCapabilities capabilities() {
        TransportCapabilities current = capabilities;
        if (current == null) {
            synchronized (this) {
                if (capabilities == null) {
                    capabilities = declare();
                }
                current = capabilities;
            }
        }
        return current;
    }

    private TransportCapabilities declare() {
        Set<OrderingScope> orderings =
                Set.of(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.PARTITION, OrderingScope.SUBSCRIPTION);
        Map<KindId, SubscriptionCapabilities> subscriptions = Map.of(
                Subscriptions.GROUP_TOPIC,
                new SubscriptionCapabilities(orderings, SettlementScope.MESSAGE, true, SystemRetry.NONE));
        return new TransportCapabilities(
                Set.of(Guarantee.AT_MOST_ONCE, Guarantee.AT_LEAST_ONCE, Guarantee.TRANSACTED),
                DeduplicationScope.SESSION,
                orderings,
                Set.of(PrefetchBound.MESSAGES, PrefetchBound.BYTES),
                subscriptions,
                false);
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        TransportCapabilities declared = capabilities();
        Declared.require(
                "Kafka",
                declared,
                options.guarantee(),
                options.deduplication(),
                options.ordering(),
                declared.orderings());
        KafkaSender.Target where = Targets.of(target);
        return env.admitted("sender.open", where.resource(), () -> {
            int partitions = topics.requirePartitions(where.topic(), where.resource());
            if (options.ordering() == OrderingScope.SUBSCRIPTION && partitions != 1) {
                throw wholeSubscriptionOrder(where.topic(), partitions);
            }
            return env.track(KafkaSender.open(env, where, options));
        });
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        Subscriptions.GroupTopic group = requireGroup(subscription);
        TransportCapabilities declared = capabilities();
        SubscriptionCapabilities kind = capabilitiesOf(declared, group);
        Declared.require(
                "Kafka", declared, options.guarantee(), DeduplicationScope.NONE, OrderingScope.NONE, kind.orderings());
        if (options.mode() == ReceiveMode.BROWSE) {
            throw KafkaErrors.unimplemented("browse through a receiver: browse the topic through the connection");
        }
        refuseFilter(options.filter().isPresent());
        return env.admitted("receiver.open", subscription, () -> {
            topics.requirePartitions(group.topic(), subscription);
            return env.track(
                    new KafkaReceiver(env, group, options, new DeadLetters(env, topics, RetryPolicy.defaults())));
        });
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions asked, MessageHandler handler) {
        Subscriptions.GroupTopic group = requireGroup(subscription);
        TransportCapabilities declared = capabilities();
        SubscriptionCapabilities kind = capabilitiesOf(declared, group);
        Retries.Resolved retry = Retries.resolve(asked.retry(), kind.retry(), asked.ordering());
        ProcessorOptions options = asked.withRetry(retry.policy());
        Declared.require(
                "Kafka",
                declared,
                options.receive().guarantee(),
                DeduplicationScope.NONE,
                options.ordering(),
                kind.orderings());
        refuseFilter(options.receive().filter().isPresent());
        if (options.stopGrace().compareTo(Consumption.REBALANCE_TIMEOUT) >= 0) {
            throw KafkaFailures.invalid("The grace time " + options.stopGrace()
                    + " must be shorter than the group's rebalance timeout " + Consumption.REBALANCE_TIMEOUT
                    + ": a member that holds a revoked partition longer is dropped from the group mid-callback.");
        }
        return env.admitted("processor.open", subscription, () -> {
            int partitions = topics.requirePartitions(group.topic(), subscription);
            if (options.ordering() == OrderingScope.SUBSCRIPTION && partitions != 1) {
                throw wholeSubscriptionOrder(group.topic(), partitions);
            }
            DeadLetters deadLetters = new DeadLetters(env, topics, options.retry());
            String id = UUID.randomUUID().toString();
            try {
                deadLetters.ensure(List.of(group.topic()));
                KafkaLanes lanes = new KafkaLanes(env, id, group, Set.of(group.topic()), options, deadLetters);
                try {
                    Processor engine = ProcessorEngine.start(
                            id,
                            options,
                            retry.inPlace(),
                            env.info(OperationType.PROCESS, subscription, Optional.of(subscription), 1),
                            env.engine(),
                            lanes,
                            handler);
                    lanes.start();
                    return env.track(new KafkaProcessor(env, engine, lanes, deadLetters));
                } catch (RuntimeException e) {
                    lanes.close();
                    throw e;
                }
            } catch (RuntimeException e) {
                deadLetters.close();
                throw e;
            }
        });
    }

    private static Subscriptions.GroupTopic requireGroup(ResourceId subscription) {
        return Subscriptions.groupTopic(subscription);
    }

    private static SubscriptionCapabilities capabilitiesOf(
            TransportCapabilities declared, Subscriptions.GroupTopic group) {
        SubscriptionCapabilities kind = declared.subscriptions().get(group.id().kind());
        if (kind == null) {
            throw KafkaErrors.unimplemented("open " + group.id().kind() + " subscriptions on this cluster");
        }
        return kind;
    }

    private static void refuseFilter(boolean filtered) {
        if (filtered) {
            throw KafkaErrors.unimplemented("filter records on the broker: Kafka has no server-side message filter");
        }
    }

    private static BrokerException wholeSubscriptionOrder(String topic, int partitions) {
        return KafkaFailures.refused(
                Status.UNIMPLEMENTED,
                ErrorReasons.NOT_SUPPORTED,
                "Kafka cannot keep the order of a whole subscription on the topic " + topic + ": it has "
                        + partitions + " partitions, and order holds only within one. Use PARTITION or GROUP ordering, "
                        + "or a topic of one partition.");
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        return env.watch(listener);
    }

    @Override
    public void close() {
        env.close();
    }
}
