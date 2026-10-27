package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * The transactions of one transactional producer, one at a time: it begins with the first send or
 * offset commit, and ends with {@link #commit} or {@link #abort}. The producer's {@code
 * transactional.id} is fixed for the life of its owner, so a restarted owner that reuses the id
 * fences the old one.
 */
final class Transaction implements AutoCloseable {

    private static final Duration CLOSE_WAIT = Duration.ofSeconds(2);

    private final KafkaProducer<byte[], byte[]> producer;
    private boolean begun;
    private volatile boolean broken;

    Transaction(KafkaCluster cluster, String transactionalId) {
        this.producer = Producers.create(cluster, Guarantee.TRANSACTED, transactionalId);
        try {
            producer.initTransactions();
        } catch (RuntimeException e) {
            producer.close(CLOSE_WAIT);
            throw e;
        }
    }

    /** Notes that the producer is fenced or failed for good: its owner replaces it before the next use. */
    void markBroken() {
        broken = true;
    }

    boolean broken() {
        return broken;
    }

    KafkaProducer<byte[], byte[]> producer() {
        return producer;
    }

    /** Begins the transaction when none is open; call before every send. */
    synchronized void ensureBegun() {
        if (!begun) {
            producer.beginTransaction();
            begun = true;
        }
    }

    /**
     * Ends the open transaction, taking {@code offsets} with it when there are any: the offsets
     * commit if and only if the sends do, fenced by the group's generation (KIP-447).
     */
    synchronized void commit(Map<TopicPartition, OffsetAndMetadata> offsets, @Nullable ConsumerGroupMetadata group) {
        if (!offsets.isEmpty()) {
            ensureBegun();
            producer.sendOffsetsToTransaction(offsets, group);
        }
        if (begun) {
            producer.commitTransaction();
            begun = false;
        }
    }

    /** Ends the open transaction without effect. */
    synchronized void abort() {
        if (begun) {
            begun = false;
            producer.abortTransaction();
        }
    }

    @Override
    public synchronized void close() {
        producer.close(CLOSE_WAIT);
    }
}
