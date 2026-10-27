package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * A pull receiver on a consumer group's subscription: a {@link Consumption} that queues what it
 * hands out for {@link #receive}. Single-owner.
 *
 * <ul>
 *   <li>{@code AT_LEAST_ONCE}: an accept settles the record, and the commit follows the lowest
 *       unsettled one. A release hands the record out again from here, after its delay. Closing
 *       without settling leaves the records to the next member.
 *   <li>{@code AT_MOST_ONCE}: the offsets are committed before the records are handed out.
 *   <li>{@code TRANSACTED}: accepts take effect on {@link #commit}, together with what the joined
 *       senders sent, in one Kafka transaction whose offsets are fenced by the group (KIP-447).
 *       {@link #rollback} aborts it and hands every record received since the last commit out again.
 * </ul>
 */
final class KafkaReceiver implements Receiver, Consuming.Sink {

    private final MessagingEnv env;
    private final Subscriptions.GroupTopic subscription;
    private final ReceiverOptions options;
    private final DeadLetters deadLetters;
    private final String id = UUID.randomUUID().toString();
    private final LinkedBlockingQueue<Delivery> queue = new LinkedBlockingQueue<>();
    private final Map<String, Delivery> outstanding = new ConcurrentHashMap<>();
    private final Consumption consumption;
    private final boolean transacted;
    private final List<Delivery> batch = new ArrayList<>();
    private final List<Delivery> accepted = new ArrayList<>();
    private final List<Sender> joined = new ArrayList<>();
    private @Nullable Transaction transaction;
    private volatile boolean closed;

    KafkaReceiver(
            MessagingEnv env, Subscriptions.GroupTopic subscription, ReceiverOptions options, DeadLetters deadLetters) {
        this.env = env;
        this.subscription = subscription;
        this.options = options;
        this.deadLetters = deadLetters;
        this.transacted = options.guarantee() == Guarantee.TRANSACTED;
        this.consumption = Consumption.create(
                env,
                id,
                new Consumption.Settings(
                        subscription.group(),
                        Set.of(subscription.topic()),
                        subscription.id(),
                        options.guarantee(),
                        options.flow().maxUnsettled(),
                        options.flow().prefetchBytes(),
                        Duration.ZERO,
                        transacted ? Consumption.Commits.EXTERNAL : Consumption.Commits.CONTIGUOUS),
                this);
        this.consumption.start();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : consumption.state();
    }

    @Override
    public void deliver(Delivery delivery) {
        queue.add(delivery);
    }

    @Override
    public void retract(Collection<TopicPartition> partitions) {
        queue.removeIf(d -> partitions.contains(d.partition()));
    }

    @Override
    public List<Message> receive(int max, Duration timeout) {
        List<Message> out = new ArrayList<>();
        long end = System.nanoTime() + timeout.toNanos();
        try {
            while (out.size() < max) {
                long left = end - System.nanoTime();
                Delivery delivery = out.isEmpty() ? queue.poll(Math.max(0, left), TimeUnit.NANOSECONDS) : queue.poll();
                if (delivery == null) {
                    break;
                }
                if (delivery.start()) {
                    outstanding.put(key(delivery.message()), delivery);
                    if (transacted) {
                        batch.add(delivery);
                    }
                    out.add(delivery.message());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        BrokerException failure = consumption.failure();
        if (out.isEmpty() && failure != null) {
            throw failure;
        }
        return out;
    }

    private static String key(Message message) {
        return message.ref().shard() + "@" + message.ref().position();
    }

    private Delivery delivery(Message message) {
        Delivery delivery = outstanding.get(key(message));
        if (delivery == null) {
            throw KafkaFailures.stale(
                    "The message " + message.ref() + " was not delivered by this receiver, or is settled.");
        }
        return delivery;
    }

    @Override
    public void settle(Message message, Disposition disposition) {
        env.settling(
                env.info(OperationType.SETTLE, subscription.id(), Optional.of(subscription.id()), 1),
                message,
                disposition,
                () -> settleObserved(message, disposition));
    }

    private void settleObserved(Message message, Disposition disposition) {
        Delivery delivery = delivery(message);
        switch (disposition) {
            case Disposition.Accept a -> accept(delivery);
            case Disposition.Release r -> {
                outstanding.remove(key(message));
                consumption.requeue(delivery, r.delay().orElse(Duration.ZERO), r.deliveryFailed());
            }
            case Disposition.Reject r -> {
                deadLetters.reject(delivery, delivery.attempt(), "rejected", transacted ? this::sender : null);
                accept(delivery);
            }
        }
    }

    private void accept(Delivery delivery) {
        if (transacted) {
            accepted.add(delivery);
        } else {
            outstanding.remove(key(delivery.message()));
            consumption.settle(delivery);
        }
    }

    @Override
    public Sender sender(SendTarget target) {
        if (!transacted) {
            throw KafkaFailures.precondition("Only a TRANSACTED receiver has a transaction to join.");
        }
        Sender sender = KafkaSender.joined(
                env, Targets.of(target), SenderOptions.defaults().withGuarantee(Guarantee.TRANSACTED), transaction());
        joined.add(sender);
        return sender;
    }

    private Transaction transaction() {
        if (transaction == null) {
            transaction = new Transaction(env.cluster(), "openbroker-receiver-" + id);
        }
        return transaction;
    }

    @Override
    public void commit() {
        if (!transacted) {
            throw KafkaFailures.precondition("Only a TRANSACTED receiver can commit.");
        }
        joined.forEach(sender -> sender.flush(env.cluster().options().timeouts().call()));
        Transaction current = transaction();
        env.admitted("receiver.commit", subscription.id(), () -> {
            consumption.commitInTransaction(current, accepted);
            return null;
        });
        accepted.forEach(d -> outstanding.remove(key(d.message())));
        batch.removeAll(accepted);
        accepted.clear();
    }

    @Override
    public void rollback() {
        if (!transacted) {
            throw KafkaFailures.precondition("Only a TRANSACTED receiver can roll back.");
        }
        if (transaction != null) {
            transaction.abort();
        }
        for (Delivery delivery : batch) {
            outstanding.remove(key(delivery.message()));
            consumption.requeue(delivery, Duration.ZERO, false);
        }
        batch.clear();
        accepted.clear();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            joined.forEach(Sender::close);
            if (transaction != null) {
                transaction.abort();
            }
        } catch (RuntimeException e) {
            // an abort that fails leaves the transaction to the broker's timeout
        } finally {
            consumption.close();
            if (transaction != null) {
                transaction.close();
            }
            env.forget(this);
        }
    }
}
