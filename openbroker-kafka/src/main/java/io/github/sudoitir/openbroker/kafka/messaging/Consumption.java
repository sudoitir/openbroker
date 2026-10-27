package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jspecify.annotations.Nullable;

/**
 * A consumer-group member: one platform thread that polls, and hands the records out as {@link
 * Delivery deliveries} to a {@link Sink}, which runs the handlers on other threads. No consumer call
 * is made from a handler thread.
 *
 * <ul>
 *   <li><b>Settlement.</b> Each partition has a {@link SettlementTracker}: a record is held from the
 *       moment it is fetched until it is settled, and the commit position is the lowest record not yet
 *       settled. Commits are asynchronous on a cadence, and synchronous on revoke and close. After a
 *       revoke, records settled beyond the first unsettled one may be delivered again.
 *   <li><b>Credit.</b> At most {@code credit} records are handed out and unsettled. Beyond that, the
 *       partitions are paused and the consumer keeps polling, so it stays in the group; fetched records
 *       wait here until credit returns.
 *   <li><b>Rebalances.</b> A revoke stops handing out the partition, lets the handlers running on it
 *       finish within the grace time, commits what they settled and lets the rest go. A lost partition
 *       is dropped at once: settling its records fails {@code DELIVERY_STALE}.
 *   <li><b>Release.</b> A record handed back is handed out again by this consumer, after its delay,
 *       with the commit position held behind it; nothing is sought.
 *   <li><b>Due times.</b> A record that carries a due time in the future (one of a retry topic) is not
 *       taken: the partition is sought back to it and paused until it is due.
 * </ul>
 */
final class Consumption implements Consuming {

    private static final System.Logger LOG = System.getLogger(Consumption.class.getName());
    private static final Duration POLL = Duration.ofMillis(20);
    private static final Duration COMMIT_EVERY = Duration.ofMillis(50);
    private static final Duration COMMIT_WAIT = Duration.ofSeconds(5);
    private static final Duration CLOSE_WAIT = Duration.ofSeconds(5);
    private static final int MAX_POLL_RECORDS = 500;

    /**
     * How long the group coordinator may go without answering a heartbeat before the link is {@code
     * RECOVERING}. The client retries a lost connection quietly and its poll never fails, so the
     * answers are the only sign that the cluster is out of reach. Brokers answer every few seconds.
     */
    private static final Duration SILENT_AFTER = Duration.ofSeconds(15);

    private static final Duration HEARTBEATS_EVERY = Duration.ofSeconds(1);
    private static final String HEARTBEAT_ANSWERS = "heartbeat-total";

    /** How long a member may go without polling before the group drops it: the group's rebalance timeout. */
    static final Duration REBALANCE_TIMEOUT = Duration.ofSeconds(60);

    /** Who commits the offsets of settled records. */
    enum Commits {
        /** The poll thread, up to the contiguous settled position. */
        CONTIGUOUS,
        /** The owner, inside a transaction. */
        EXTERNAL
    }

    /**
     * @param group the group id
     * @param topics the topics to read
     * @param source the subscription the messages say they came from
     * @param guarantee {@code AT_MOST_ONCE} commits before a record is handed out
     * @param credit the most records handed out and unsettled
     * @param prefetchBytes the bytes the client may fetch ahead, or 0 for its default
     * @param grace how long a revoke waits for the handlers running on the partition
     */
    record Settings(
            String group,
            Set<String> topics,
            ResourceId source,
            Guarantee guarantee,
            int credit,
            long prefetchBytes,
            Duration grace,
            Commits commits) {}

    private static final class Tracking {
        final Delivery.Assignment assignment = new Delivery.Assignment();
        final SettlementTracker tracker = new SettlementTracker();
        final TreeMap<Long, Delivery> held = new TreeMap<>();
    }

    private record Requeue(Delivery delivery, long dueNanos, boolean failed) {}

    private final Settings settings;
    private final Consuming.Sink sink;
    private final Consumer<byte[], byte[]> consumer;
    private final Thread thread;
    private final Map<TopicPartition, Tracking> tracking = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Requeue> inbox = new ConcurrentLinkedQueue<>();
    private final PriorityQueue<Requeue> timed = new PriorityQueue<>((a, b) -> Long.compare(a.dueNanos, b.dueNanos));
    private final Map<TopicPartition, Long> delayed = new HashMap<>();
    private final Map<TopicPartition, Long> issued = new HashMap<>();
    private final ReentrantLock commitLock = new ReentrantLock();
    private volatile @Nullable ConsumerGroupMetadata groupMetadata;
    private volatile @Nullable BrokerException failure;
    private volatile boolean closing;
    private volatile boolean ended;
    private volatile long lastAnswer = System.nanoTime();
    private long lastHeartbeatCount;
    private long lastHeartbeatCheck;
    private long lastCommit;
    private int cursor;

    /** A member of the group over {@code consumer}, which only the poll thread touches once {@link #start}ed. */
    Consumption(String id, Settings settings, Consuming.Sink sink, Consumer<byte[], byte[]> consumer) {
        this.settings = settings;
        this.sink = sink;
        this.consumer = consumer;
        this.thread = new Thread(this::run, "openbroker-kafka-poll-" + id);
        this.thread.setDaemon(true);
    }

    /** A member of the group, over a Kafka consumer built from the connection's settings. */
    static Consumption create(MessagingEnv env, String id, Settings settings, Consuming.Sink sink) {
        return new Consumption(id, settings, sink, new KafkaConsumer<>(config(env, id, settings)));
    }

    private static Map<String, Object> config(MessagingEnv env, String id, Settings settings) {
        KafkaCluster cluster = env.cluster();
        Map<String, Object> config = new HashMap<>(cluster.clientConfig(Plane.DATA));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, config.get(ConsumerConfig.CLIENT_ID_CONFIG) + "-" + id);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, settings.group());
        config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.min(settings.credit(), MAX_POLL_RECORDS));
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) REBALANCE_TIMEOUT.toMillis());
        if (settings.prefetchBytes() > 0) {
            int bytes = (int) Math.min(Integer.MAX_VALUE, Math.max(1024, settings.prefetchBytes()));
            config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bytes);
            config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, bytes);
        }
        return config;
    }

    /** The listener the consumer calls on a rebalance; tests call it to stand for the group. */
    ConsumerRebalanceListener rebalances() {
        return new Rebalances();
    }

    @Override
    public void start() {
        thread.start();
    }

    @Override
    public LinkState state() {
        if (closing || ended) {
            return LinkState.CLOSED;
        }
        return System.nanoTime() - lastAnswer > SILENT_AFTER.toNanos() ? LinkState.RECOVERING : LinkState.OPEN;
    }

    /** The failure the last poll ended with (a denied topic, a rejected login), or null while polling works. */
    @Override
    public @Nullable BrokerException failure() {
        return failure;
    }

    @Nullable ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    // ---- settling, from any thread -----------------------------------------------------------

    @Override
    public void settle(Delivery delivery) {
        Tracking t = requireCurrent(delivery);
        delivery.finish();
        t.tracker.settled(delivery.offset());
    }

    /** A rejected record is settled: its dead letter, if any, was sent first. */
    @Override
    public void reject(Delivery delivery) {
        settle(delivery);
    }

    @Override
    public void requeue(Delivery delivery, Duration delay, boolean failed) {
        requireCurrent(delivery);
        delivery.finish();
        inbox.add(new Requeue(delivery, System.nanoTime() + delay.toNanos(), failed));
    }

    private Tracking requireCurrent(Delivery delivery) {
        Tracking t = tracking.get(delivery.partition());
        if (!delivery.current() || t == null || t.assignment != delivery.assignment()) {
            throw staleRecord(delivery);
        }
        return t;
    }

    private static BrokerException staleRecord(Delivery delivery) {
        return KafkaFailures.stale("The partition " + delivery.partition()
                + " is no longer this consumer's; its records are delivered again to the new owner.");
    }

    /**
     * Ends {@code transaction} with the offsets of {@code settled}, which commit if and only if the
     * transaction's sends do, and then settles them. A delivery of a lost partition is refused.
     */
    void commitInTransaction(Transaction transaction, List<Delivery> settled) {
        commitLock.lock();
        try {
            Map<TopicPartition, List<Long>> byPartition = new HashMap<>();
            Map<TopicPartition, Tracking> claimed = new HashMap<>();
            for (Delivery delivery : settled) {
                // One look at each partition: a rebalance on the poll thread may drop it at any moment.
                Tracking t = claimed.computeIfAbsent(delivery.partition(), tracking::get);
                if (!delivery.current() || t == null || t.assignment != delivery.assignment()) {
                    throw staleRecord(delivery);
                }
                byPartition
                        .computeIfAbsent(delivery.partition(), p -> new ArrayList<>())
                        .add(delivery.offset());
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            byPartition.forEach((partition, list) -> claimed.get(partition)
                    .tracker
                    .pendingCommitAfter(list)
                    .ifPresent(position -> offsets.put(partition, new OffsetAndMetadata(position))));
            transaction.commit(offsets, groupMetadata);
            for (Delivery delivery : settled) {
                delivery.finish();
                claimed.get(delivery.partition()).tracker.settled(delivery.offset());
            }
            offsets.forEach(
                    (partition, offset) -> claimed.get(partition).tracker.committed(offset.offset()));
        } finally {
            commitLock.unlock();
        }
    }

    // ---- the poll thread ------------------------------------------------------------------------

    private void run() {
        try {
            consumer.subscribe(settings.topics(), rebalances());
            while (!closing) {
                try {
                    drainInbox();
                    releaseDue();
                    dispatch();
                    pauseAndResume();
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL);
                    groupMetadata = consumer.groupMetadata();
                    failure = null;
                    watchCoordinator();
                    ingest(records);
                    dispatch();
                    commitDue();
                } catch (InterruptException e) {
                    return;
                } catch (RuntimeException e) {
                    failure = KafkaFailures.of(e, Optional.of(settings.source()));
                    LOG.log(System.Logger.Level.DEBUG, "A poll failed; trying again", e);
                    sleep(Duration.ofMillis(200));
                }
            }
        } finally {
            shutdown();
        }
    }

    /** Notes that the coordinator answered a heartbeat since the last look, which is what keeps the link open. */
    private void watchCoordinator() {
        long now = System.nanoTime();
        if (now - lastHeartbeatCheck < HEARTBEATS_EVERY.toNanos()) {
            return;
        }
        lastHeartbeatCheck = now;
        for (var metric : consumer.metrics().entrySet()) {
            if (metric.getKey().name().equals(HEARTBEAT_ANSWERS)
                    && metric.getValue().metricValue() instanceof Number count
                    && count.longValue() != lastHeartbeatCount) {
                lastHeartbeatCount = count.longValue();
                lastAnswer = now;
            }
        }
    }

    private void drainInbox() {
        Requeue requeue;
        while ((requeue = inbox.poll()) != null) {
            timed.add(requeue);
        }
    }

    /** Puts the handed-back deliveries whose delay has passed back among the waiting ones. */
    private void releaseDue() {
        long now = System.nanoTime();
        while (!timed.isEmpty() && timed.peek().dueNanos - now <= 0) {
            Requeue requeue = timed.poll();
            Delivery old = requeue.delivery();
            Tracking t = tracking.get(old.partition());
            if (t == null || t.assignment != old.assignment() || !old.current()) {
                continue;
            }
            int attempt = old.attempt() + (requeue.failed() ? 1 : 0);
            Delivery again = new Delivery(message(old.record(), attempt), old.record(), attempt, t.assignment);
            t.held.put(again.offset(), again);
        }
    }

    /** Hands out waiting records, a partition at a time, while there is credit. */
    private void dispatch() {
        List<Tracking> partitions = new ArrayList<>(tracking.values());
        if (partitions.isEmpty()) {
            return;
        }
        boolean progress = true;
        while (progress && handedOut() < settings.credit()) {
            progress = false;
            for (int i = 0; i < partitions.size() && handedOut() < settings.credit(); i++) {
                Tracking t = partitions.get(Math.floorMod(cursor + i, partitions.size()));
                var first = t.held.pollFirstEntry();
                if (first != null) {
                    sink.deliver(first.getValue());
                    progress = true;
                }
            }
            cursor++;
        }
    }

    /** The records handed out and not settled: every unsettled one that is not waiting here. */
    private int handedOut() {
        int count = 0;
        for (Tracking t : tracking.values()) {
            count += t.tracker.unsettled() - t.held.size();
        }
        return count;
    }

    /** Pauses every partition while records wait or credit is spent, and the partitions waiting for a due time. */
    private void pauseAndResume() {
        long now = System.nanoTime();
        delayed.values().removeIf(due -> due - now <= 0);
        Set<TopicPartition> assigned = consumer.assignment();
        boolean backlog =
                handedOut() >= settings.credit() || tracking.values().stream().anyMatch(t -> !t.held.isEmpty());
        Set<TopicPartition> want = new HashSet<>();
        if (backlog) {
            want.addAll(assigned);
        }
        delayed.keySet().stream().filter(assigned::contains).forEach(want::add);
        Set<TopicPartition> paused = consumer.paused();
        Set<TopicPartition> pause = new HashSet<>(want);
        pause.removeAll(paused);
        Set<TopicPartition> resume = new HashSet<>(paused);
        resume.removeAll(want);
        if (!pause.isEmpty()) {
            consumer.pause(pause);
        }
        if (!resume.isEmpty()) {
            consumer.resume(resume);
        }
    }

    private void ingest(ConsumerRecords<byte[], byte[]> records) {
        if (records.isEmpty()) {
            return;
        }
        if (settings.guarantee() == Guarantee.AT_MOST_ONCE) {
            commitBeforeHandOut(records);
        }
        long now = System.currentTimeMillis();
        for (TopicPartition partition : records.partitions()) {
            Tracking t = tracking.get(partition);
            if (t == null) {
                continue;
            }
            for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                long due = number(record, RecordCodec.DUE).orElse(0);
                if (due > now) {
                    consumer.seek(partition, record.offset());
                    delayed.put(
                            partition,
                            System.nanoTime() + Duration.ofMillis(due - now).toNanos());
                    break;
                }
                int attempt = (int) number(record, RecordCodec.ATTEMPT).orElse(0) + 1;
                // The message first: a record that cannot be turned into one must not be left half taken.
                Message message = message(record, attempt);
                t.tracker.fetched(record.offset());
                Delivery delivery = new Delivery(message, record, attempt, t.assignment);
                t.held.put(record.offset(), delivery);
            }
        }
    }

    /** A header holding a number, or empty when it is missing or not one: any producer can set a header. */
    static OptionalLong number(ConsumerRecord<byte[], byte[]> record, String header) {
        try {
            return RecordCodec.header(record, header)
                    .map(text -> OptionalLong.of(Long.parseLong(text.trim())))
                    .orElse(OptionalLong.empty());
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    /**
     * The message for a record. A record whose headers the codec cannot read is handed out with its
     * key, value and no headers, rather than lost.
     */
    private Message message(ConsumerRecord<byte[], byte[]> record, int attempt) {
        try {
            return RecordCodec.message(settings.source(), record, Long.MAX_VALUE, attempt);
        } catch (RuntimeException e) {
            LOG.log(
                    System.Logger.Level.WARNING,
                    "A record's headers could not be read; it is handed out without them",
                    e);
            ConsumerRecord<byte[], byte[]> bare = new ConsumerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.serializedKeySize(),
                    record.serializedValueSize(),
                    record.key(),
                    record.value(),
                    new RecordHeaders(),
                    record.leaderEpoch());
            return RecordCodec.message(settings.source(), bare, Long.MAX_VALUE, attempt);
        }
    }

    private void commitBeforeHandOut(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> list = records.records(partition);
            offsets.put(partition, new OffsetAndMetadata(list.getLast().offset() + 1));
        }
        consumer.commitSync(offsets, COMMIT_WAIT);
    }

    private void commitDue() {
        if (settings.commits() != Commits.CONTIGUOUS || settings.guarantee() == Guarantee.AT_MOST_ONCE) {
            return;
        }
        long now = System.nanoTime();
        if (now - lastCommit < COMMIT_EVERY.toNanos()) {
            return;
        }
        lastCommit = now;
        Map<TopicPartition, OffsetAndMetadata> offsets = pendingOffsets();
        offsets.keySet()
                .removeIf(p -> issued.getOrDefault(p, -1L) >= offsets.get(p).offset());
        if (offsets.isEmpty()) {
            return;
        }
        offsets.forEach((p, o) -> issued.put(p, o.offset()));
        consumer.commitAsync(offsets, (committed, error) -> {
            if (error == null) {
                committed.forEach((p, o) -> {
                    Tracking t = tracking.get(p);
                    if (t != null) {
                        t.tracker.committed(o.offset());
                    }
                });
            } else {
                committed.keySet().forEach(issued::remove);
            }
        });
    }

    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        tracking.forEach((partition, t) -> t.tracker
                .pendingCommit()
                .ifPresent(position -> offsets.put(partition, new OffsetAndMetadata(position))));
        return offsets;
    }

    private void commitNow(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty() || settings.commits() != Commits.CONTIGUOUS) {
            return;
        }
        try {
            consumer.commitSync(offsets, COMMIT_WAIT);
            offsets.forEach((p, o) -> {
                Tracking t = tracking.get(p);
                if (t != null) {
                    t.tracker.committed(o.offset());
                }
            });
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "A commit failed; its records are delivered again", e);
        }
    }

    private void shutdown() {
        try {
            if (settings.guarantee() != Guarantee.AT_MOST_ONCE) {
                commitNow(pendingOffsets());
            }
        } finally {
            try {
                consumer.close(CLOSE_WAIT);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "The consumer failed to close", e);
            }
            tracking.values().forEach(t -> t.assignment.end());
            ended = true;
        }
    }

    private static void sleep(Duration time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closing = true;
        if (thread.isAlive()) {
            try {
                thread.join(CLOSE_WAIT.plusSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ---- rebalances -------------------------------------------------------------------------------

    private final class Rebalances implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                tracking.putIfAbsent(partition, new Tracking());
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            List<Tracking> going = stopHandingOut(partitions);
            sink.retract(partitions);
            long end = System.nanoTime() + settings.grace().toNanos();
            while (System.nanoTime() - end < 0 && going.stream().anyMatch(t -> t.assignment.running() > 0)) {
                sleep(Duration.ofMillis(5));
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                Tracking t = tracking.get(partition);
                if (t != null) {
                    t.tracker.pendingCommit().ifPresent(p -> offsets.put(partition, new OffsetAndMetadata(p)));
                }
            }
            commitNow(offsets);
            drop(partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            stopHandingOut(partitions);
            sink.retract(partitions);
            drop(partitions);
        }
    }

    /** Stops handing out the partitions: nothing queued is taken, nothing waiting is delivered. */
    private List<Tracking> stopHandingOut(Collection<TopicPartition> partitions) {
        List<Tracking> going = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            delayed.remove(partition);
            issued.remove(partition);
            Tracking t = tracking.get(partition);
            if (t != null) {
                t.assignment.revoking();
                t.held.clear();
                going.add(t);
            }
        }
        return going;
    }

    /** Ends the claim on the partitions: from now on settling their records fails stale. */
    private void drop(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Tracking t = tracking.remove(partition);
            if (t != null) {
                t.assignment.end();
            }
        }
    }
}
