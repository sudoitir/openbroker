package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * The lanes of one processor on a consumer group or share group: one {@link Consuming} polls for all of them, and
 * a record goes to the queue of one lane. The routing is the ordering:
 *
 * <ul>
 *   <li>{@code PARTITION}: a partition's records all go to the lane the partition hashes to.
 *   <li>{@code GROUP}: a record goes to the lane its group (or key) hashes to within its partition;
 *       a record without either is its own group.
 *   <li>{@code SUBSCRIPTION}: one lane, on a one-partition topic.
 *   <li>{@code NONE}: one queue, from which whichever lane is free takes the next record.
 * </ul>
 *
 * Each lane runs one record at a time, so a lane's records stay in order and the system pins groups to
 * lanes: {@link #pinsGroups()} is true and the engine takes no locks of its own.
 *
 * <p>A {@code TRANSACTED} lane has a producer of its own with a {@code transactional.id} that is fixed
 * for the processor's life. Accepting a record commits the transaction with the record's offset,
 * fenced by the group's generation; releasing it aborts the transaction and hands the record out
 * again.
 */
final class KafkaLanes implements ProcessorEngine.Lanes, Consuming.Sink, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(KafkaLanes.class.getName());

    private final MessagingEnv env;
    private final String processorId;
    private final Subscriptions.GroupTopic subscription;
    private final ProcessorOptions options;
    private final DeadLetters deadLetters;
    private final @Nullable RetryTopics retryTopics;
    private final boolean brokerDeadLetters;
    private final boolean transacted;
    private final List<LinkedBlockingQueue<Delivery>> queues = new CopyOnWriteArrayList<>();
    private final AtomicInteger next = new AtomicInteger();
    private final List<Lane> lanes = new CopyOnWriteArrayList<>();
    private final Consuming consuming;

    /**
     * @param retryTopics where an unordered managed retry waits, or null to hold it here
     * @param brokerDeadLetters whether the broker dead-letters what is rejected, so the lane does not
     * @param consuming makes the group member that feeds the lanes, given where to hand records
     */
    KafkaLanes(
            MessagingEnv env,
            String processorId,
            Subscriptions.GroupTopic subscription,
            ProcessorOptions options,
            DeadLetters deadLetters,
            @Nullable RetryTopics retryTopics,
            boolean brokerDeadLetters,
            Function<Consuming.Sink, Consuming> consuming) {
        this.env = env;
        this.processorId = processorId;
        this.subscription = subscription;
        this.options = options;
        this.deadLetters = deadLetters;
        this.retryTopics = retryTopics;
        this.brokerDeadLetters = brokerDeadLetters;
        this.transacted = options.receive().guarantee() == Guarantee.TRANSACTED;
        // Unordered lanes take from one queue, so whichever is free takes the next record.
        int queueCount = options.ordering() == OrderingScope.NONE ? 1 : options.concurrency();
        for (int i = 0; i < queueCount; i++) {
            queues.add(new LinkedBlockingQueue<>());
        }
        this.consuming = consuming.apply(this);
    }

    void start() {
        consuming.start();
    }

    @Override
    public ProcessorEngine.Lane open(int index) {
        Lane lane = new Lane(index);
        lanes.add(lane);
        return lane;
    }

    @Override
    public boolean pinsGroups() {
        return true;
    }

    @Override
    public LinkState state() {
        return consuming.state();
    }

    @Override
    public Sender deadLetter(SendTarget target) {
        return deadLetters.sender(target);
    }

    // ---- routing, on the poll thread ------------------------------------------------------------

    @Override
    public void deliver(Delivery delivery) {
        queues.get(laneOf(delivery)).add(delivery);
    }

    @Override
    public void retract(Collection<TopicPartition> partitions) {
        for (LinkedBlockingQueue<Delivery> queue : queues) {
            queue.removeIf(d -> partitions.contains(d.partition()));
        }
    }

    private int laneOf(Delivery delivery) {
        int n = queues.size();
        if (n == 1) {
            return 0;
        }
        Message message = delivery.message();
        return switch (options.ordering()) {
            case SUBSCRIPTION -> 0;
            case PARTITION -> Math.floorMod(delivery.partition().hashCode(), n);
            case GROUP -> {
                Optional<String> group = message.headers().groupId();
                int hash;
                if (group.isPresent()) {
                    hash = group.get().hashCode();
                } else if (message.headers().key().isPresent()) {
                    hash = Arrays.hashCode(message.headers().key().get().toByteArray());
                } else {
                    yield Math.floorMod(next.getAndIncrement(), n);
                }
                yield Math.floorMod(31 * delivery.partition().hashCode() + hash, n);
            }
            case NONE -> 0;
        };
    }

    // ---- one lane --------------------------------------------------------------------------------

    private final class Lane implements ProcessorEngine.Lane {

        private final int index;
        private final @Nullable Transaction transaction;
        private final Map<String, Sender> joined = new HashMap<>();
        private @Nullable Delivery current;

        Lane(int index) {
            this.index = index;
            this.transaction = transacted
                    ? new Transaction(
                            env.cluster(), "openbroker-" + subscription.group() + "-" + processorId + "-" + index)
                    : null;
        }

        @Override
        public Optional<Message> next(Duration timeout) throws InterruptedException {
            Delivery delivery =
                    queues.get(Math.min(index, queues.size() - 1)).poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (delivery == null) {
                BrokerException failure = consuming.failure();
                if (failure != null) {
                    throw failure;
                }
                return Optional.empty();
            }
            if (!delivery.start()) {
                return Optional.empty();
            }
            current = delivery;
            return Optional.of(delivery.message());
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
            Delivery delivery = current;
            current = null;
            if (delivery == null || delivery.message() != message) {
                throw KafkaFailures.stale("The message " + message.ref() + " is not the one this lane holds.");
            }
            if (transaction != null) {
                settleInTransaction(delivery, disposition, transaction);
                return;
            }
            switch (disposition) {
                case Disposition.Accept a -> consuming.settle(delivery);
                case Disposition.Release r -> release(delivery, r);
                case Disposition.Reject r -> {
                    if (!brokerDeadLetters) {
                        deadLetters.reject(
                                delivery, delivery.attempt(), r.reason().orElse("rejected"), null);
                    }
                    consuming.reject(delivery);
                }
            }
        }

        /**
         * A failed record with a delay goes to its retry topic, so its partition keeps moving; any
         * other release, and a retry topic that does not take it, hand it back to this consumer.
         */
        private void release(Delivery delivery, Disposition.Release release) {
            if (retryTopics != null && release.delay().isPresent() && release.deliveryFailed()) {
                try {
                    retryTopics.release(delivery);
                    consuming.settle(delivery);
                    return;
                } catch (BrokerException e) {
                    if (ErrorReasons.DELIVERY_STALE.equals(e.reason())) {
                        throw e;
                    }
                    LOG.log(System.Logger.Level.WARNING, "A retry topic did not take a record; retrying it here", e);
                }
            }
            consuming.requeue(delivery, release.delay().orElse(Duration.ZERO), release.deliveryFailed());
        }

        private void settleInTransaction(Delivery delivery, Disposition disposition, Transaction txn) {
            switch (disposition) {
                case Disposition.Accept a -> commit(delivery, txn);
                case Disposition.Reject r -> {
                    try {
                        deadLetters.reject(
                                delivery, delivery.attempt(), r.reason().orElse("rejected"), this::sender);
                    } catch (RuntimeException e) {
                        handBack(delivery, txn, true);
                        throw e;
                    }
                    commit(delivery, txn);
                }
                case Disposition.Release r -> {
                    abort(txn);
                    consuming.requeue(delivery, r.delay().orElse(Duration.ZERO), r.deliveryFailed());
                }
            }
        }

        private void commit(Delivery delivery, Transaction txn) {
            try {
                joined.values()
                        .forEach(sender ->
                                sender.flush(env.cluster().options().timeouts().call()));
                env.admitted("processor.commit", subscription.id(), () -> {
                    ((Consumption) consuming).commitInTransaction(txn, List.of(delivery));
                    return null;
                });
            } catch (RuntimeException e) {
                handBack(delivery, txn, true);
                throw e;
            }
        }

        /** The transaction failed or was refused: end it, and have the record delivered again if it can be. */
        private void handBack(Delivery delivery, Transaction txn, boolean failed) {
            abort(txn);
            try {
                consuming.requeue(delivery, Duration.ZERO, failed);
            } catch (BrokerException stale) {
                // the partition moved on; the new owner has the record
            }
        }

        private void abort(Transaction txn) {
            try {
                txn.abort();
            } catch (RuntimeException e) {
                // a transaction that cannot be aborted ends at the broker's timeout
            }
        }

        @Override
        public Sender sender(SendTarget target) {
            Transaction txn = transaction;
            if (txn == null) {
                throw KafkaFailures.precondition("Only a TRANSACTED processor has a transaction to join.");
            }
            KafkaSender.Target where = Targets.of(target);
            return joined.computeIfAbsent(
                    where.resource().format() + "#" + where.partition() + "#" + where.routingKey(),
                    k -> KafkaSender.joined(
                            env, where, SenderOptions.defaults().withGuarantee(Guarantee.TRANSACTED), txn));
        }

        @Override
        public void close() {
            joined.values().forEach(Sender::close);
            if (transaction != null) {
                abort(transaction);
                transaction.close();
            }
        }
    }

    /** Stops polling, commits what was settled and leaves the group. */
    @Override
    public void close() {
        consuming.close();
        List<ProcessorEngine.Lane> open = new ArrayList<>(lanes);
        open.forEach(ProcessorEngine.Lane::close);
    }
}
