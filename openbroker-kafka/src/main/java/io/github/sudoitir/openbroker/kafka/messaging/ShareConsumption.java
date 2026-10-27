package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jspecify.annotations.Nullable;

/**
 * A member of a share group: one platform thread that polls a {@link KafkaShareConsumer} in explicit
 * acknowledgement mode and hands the records out as {@link Delivery deliveries}, as {@link
 * Consumption} does for a consumer group. A record is acknowledged by what the handler decided:
 * accept, release or reject; the broker counts the deliveries.
 *
 * <p>A record is held by an acquisition lock that the broker releases when it runs out. The thread
 * renews the lock of every record still being handled, and of every record waiting for credit, at half
 * the lock time ({@code AcknowledgeType.RENEW}); a renewed record comes back from the next poll and is
 * recognised, not handed out twice. The renewal is the poll loop's own: it has no API.
 *
 * <p>A release with a delay keeps renewing the record until the delay has passed, then releases it.
 * The consumer is polled only when something finished, when a renewal is due, or, while there is
 * credit to fill, every {@value #FILL_MS} ms.
 */
final class ShareConsumption implements Consuming {

    private static final System.Logger LOG = System.getLogger(ShareConsumption.class.getName());
    private static final Duration POLL = Duration.ofMillis(50);
    private static final Duration IDLE_POLL = Duration.ofMillis(200);
    private static final Duration CLOSE_WAIT = Duration.ofSeconds(5);
    private static final int FILL_MS = 500;
    private static final int DEFAULT_LOCK_MS = 30_000;
    private static final int MAX_POLL_RECORDS = 500;

    /**
     * @param group the share group id
     * @param topics the topics to read
     * @param source the subscription the messages say they came from
     * @param credit the most records handed out and unsettled
     */
    record Settings(String group, Set<String> topics, ResourceId source, int credit) {}

    private record Outcome(AcknowledgeType type, long dueNanos) {}

    private static final class Held {
        final Delivery delivery;
        boolean handedOut;

        Held(Delivery delivery) {
            this.delivery = delivery;
        }
    }

    private final Settings settings;
    private final Consuming.Sink sink;
    private final KafkaShareConsumer<byte[], byte[]> consumer;
    private final Thread thread;
    private final Delivery.Assignment assignment = new Delivery.Assignment();
    private final Map<String, Outcome> outcomes = new ConcurrentHashMap<>();
    private final Map<String, Held> holding = new HashMap<>();
    private final ArrayDeque<Delivery> waiting = new ArrayDeque<>();
    private final Semaphore wake = new Semaphore(0);
    private List<ConsumerRecord<byte[], byte[]>> lastPoll = List.of();
    private volatile @Nullable BrokerException failure;
    private final CoordinatorWatch coordinator = new CoordinatorWatch();
    private volatile boolean closing;
    private volatile boolean ended;
    private long lastRenewal = System.nanoTime();

    ShareConsumption(String id, Settings settings, Consuming.Sink sink, KafkaShareConsumer<byte[], byte[]> consumer) {
        this.settings = settings;
        this.sink = sink;
        this.consumer = consumer;
        this.thread = new Thread(this::run, "openbroker-kafka-share-" + id);
        this.thread.setDaemon(true);
    }

    /** A member of the share group, over a share consumer built from the connection's settings. */
    static ShareConsumption create(MessagingEnv env, String id, Settings settings, Consuming.Sink sink) {
        KafkaCluster cluster = env.cluster();
        Map<String, Object> config = new HashMap<>(cluster.clientConfig(Plane.DATA));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, config.get(ConsumerConfig.CLIENT_ID_CONFIG) + "-" + id);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, settings.group());
        config.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        config.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.min(settings.credit(), MAX_POLL_RECORDS));
        return new ShareConsumption(id, settings, sink, new KafkaShareConsumer<>(config));
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
        return coordinator.silent() ? LinkState.RECOVERING : LinkState.OPEN;
    }

    @Override
    public @Nullable BrokerException failure() {
        return failure;
    }

    // ---- settling, from any thread -----------------------------------------------------------

    @Override
    public void settle(Delivery delivery) {
        decide(delivery, AcknowledgeType.ACCEPT, Duration.ZERO);
    }

    @Override
    public void reject(Delivery delivery) {
        decide(delivery, AcknowledgeType.REJECT, Duration.ZERO);
    }

    /** The broker counts the delivery on its own, so {@code failed} changes nothing here. */
    @Override
    public void requeue(Delivery delivery, Duration delay, boolean failed) {
        decide(delivery, AcknowledgeType.RELEASE, delay);
    }

    private void decide(Delivery delivery, AcknowledgeType type, Duration delay) {
        if (!delivery.current()) {
            throw KafkaFailures.stale("The share group member is closed; the broker delivers its records again.");
        }
        delivery.finish();
        outcomes.put(key(delivery.record()), new Outcome(type, System.nanoTime() + delay.toNanos()));
        wake.release();
    }

    private static String key(ConsumerRecord<byte[], byte[]> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }

    // ---- the poll thread ------------------------------------------------------------------------

    private void run() {
        try {
            consumer.setAcknowledgementCommitCallback((offsets, error) -> {
                if (error != null) {
                    LOG.log(System.Logger.Level.DEBUG, "An acknowledgement was not accepted: " + offsets, error);
                }
            });
            consumer.subscribe(settings.topics());
            while (!closing) {
                try {
                    acknowledgePrevious();
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(holding.isEmpty() ? IDLE_POLL : POLL);
                    failure = null;
                    coordinator.look(consumer.metrics());
                    take(records);
                    dispatch();
                    pause();
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

    /** Acknowledges every record of the last poll: what was decided, and a renewal for the rest. */
    private void acknowledgePrevious() {
        long now = System.nanoTime();
        boolean renewing = false;
        List<ConsumerRecord<byte[], byte[]>> pending = lastPoll;
        lastPoll = List.of();
        for (ConsumerRecord<byte[], byte[]> record : pending) {
            String key = key(record);
            Outcome outcome = outcomes.get(key);
            if (outcome != null && outcome.dueNanos - now <= 0) {
                outcomes.remove(key);
                holding.remove(key);
                consumer.acknowledge(record, outcome.type);
            } else {
                consumer.acknowledge(record, AcknowledgeType.RENEW);
                renewing = true;
            }
        }
        if (renewing) {
            lastRenewal = now;
        }
    }

    /** Takes the records a poll returned: the new ones wait for credit, the renewed ones are recognised. */
    private void take(ConsumerRecords<byte[], byte[]> records) {
        List<ConsumerRecord<byte[], byte[]>> returned = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            returned.add(record);
            if (!holding.containsKey(key(record))) {
                int attempt = record.deliveryCount().map(Short::intValue).orElse(1);
                Delivery delivery = new Delivery(
                        RecordCodec.message(settings.source(), record, Long.MAX_VALUE, attempt),
                        record,
                        attempt,
                        assignment);
                holding.put(key(record), new Held(delivery));
                waiting.addLast(delivery);
            }
        }
        lastPoll = returned;
    }

    private void dispatch() {
        while (!waiting.isEmpty() && handedOut() < settings.credit()) {
            Delivery delivery = waiting.pollFirst();
            holding.get(key(delivery.record())).handedOut = true;
            sink.deliver(delivery);
        }
    }

    private int handedOut() {
        int count = 0;
        for (Held held : holding.values()) {
            if (held.handedOut) {
                count++;
            }
        }
        return count;
    }

    /**
     * Waits until a record is settled, a renewal is due, or there is credit to fill. The consumer is
     * not polled meanwhile: a renewed record would come straight back and the loop would spin.
     */
    private void pause() {
        if (holding.isEmpty()) {
            return;
        }
        long renewEvery = TimeUnit.MILLISECONDS.toNanos(lockMs() / 2);
        long untilRenewal = lastRenewal + renewEvery - System.nanoTime();
        long untilDue = nextDue();
        boolean credit = handedOut() + waiting.size() < settings.credit();
        long wait = Math.min(untilRenewal, credit ? TimeUnit.MILLISECONDS.toNanos(FILL_MS) : untilRenewal);
        wait = Math.min(wait, untilDue);
        if (wait > 0) {
            try {
                wake.tryAcquire(wait, TimeUnit.NANOSECONDS);
                wake.drainPermits();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Nanoseconds until the earliest delayed release is due; long if none waits. */
    private long nextDue() {
        long now = System.nanoTime();
        long soonest = Long.MAX_VALUE;
        for (Outcome outcome : outcomes.values()) {
            soonest = Math.min(soonest, outcome.dueNanos - now);
        }
        return soonest;
    }

    private int lockMs() {
        return consumer.acquisitionLockTimeoutMs().orElse(DEFAULT_LOCK_MS);
    }

    private void shutdown() {
        try {
            for (ConsumerRecord<byte[], byte[]> record : lastPoll) {
                Outcome outcome = outcomes.get(key(record));
                try {
                    consumer.acknowledge(
                            record,
                            outcome != null && outcome.dueNanos - System.nanoTime() <= 0
                                    ? outcome.type
                                    : AcknowledgeType.RELEASE);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.DEBUG, "A record could not be acknowledged on close", e);
                }
            }
            consumer.commitSync(CLOSE_WAIT);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "The last acknowledgements were not committed", e);
        } finally {
            try {
                consumer.close(CLOSE_WAIT);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "The consumer failed to close", e);
            }
            assignment.end();
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
        wake.release();
        if (thread.isAlive()) {
            try {
                thread.join(CLOSE_WAIT.plusSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
