package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.messaging.Message;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * One record handed out for handling, with the claim on its partition it was handed out under. It is
 * queued until a handler takes it ({@link #start}), and done once it is settled or handed back
 * ({@link #finish}).
 */
final class Delivery {

    /** The claim a consumer holds on one partition, from its assignment until it is revoked or lost. */
    static final class Assignment {
        private volatile boolean valid = true;
        private volatile boolean revoking;
        private final AtomicInteger running = new AtomicInteger();

        boolean usable() {
            return valid && !revoking;
        }

        boolean valid() {
            return valid;
        }

        int running() {
            return running.get();
        }

        void revoking() {
            revoking = true;
        }

        void end() {
            valid = false;
        }
    }

    private static final int QUEUED = 0;
    private static final int RUNNING = 1;
    private static final int DONE = 2;

    private final Message message;
    private final ConsumerRecord<byte[], byte[]> record;
    private final int attempt;
    private final Assignment assignment;
    private final AtomicInteger state = new AtomicInteger(QUEUED);

    Delivery(Message message, ConsumerRecord<byte[], byte[]> record, int attempt, Assignment assignment) {
        this.message = message;
        this.record = record;
        this.attempt = attempt;
        this.assignment = assignment;
    }

    Message message() {
        return message;
    }

    ConsumerRecord<byte[], byte[]> record() {
        return record;
    }

    TopicPartition partition() {
        return new TopicPartition(record.topic(), record.partition());
    }

    /** The topic the record was first read from: its own, or the one it went to a retry topic from. */
    String originTopic() {
        var kafka = message.extension(Records.EXTENSION);
        return String.valueOf(
                kafka.raw("origin_topic").or(() -> kafka.raw("topic")).orElse(record.topic()));
    }

    long offset() {
        return record.offset();
    }

    /** The 1-based delivery attempt. */
    int attempt() {
        return attempt;
    }

    Assignment assignment() {
        return assignment;
    }

    /** Whether a handler may still take this: it is queued and its partition is still this consumer's. */
    boolean queued() {
        return state.get() == QUEUED && assignment.usable();
    }

    /** Takes the delivery for a handler; false when it was taken, finished, or its partition is going. */
    boolean start() {
        if (!assignment.usable() || !state.compareAndSet(QUEUED, RUNNING)) {
            return false;
        }
        assignment.running.incrementAndGet();
        if (!assignment.usable()) {
            finish();
            return false;
        }
        return true;
    }

    /** Ends the handler's claim; true the first time. */
    boolean finish() {
        if (state.getAndSet(DONE) == RUNNING) {
            assignment.running.decrementAndGet();
            return true;
        }
        return false;
    }

    /** Settling is refused once the partition is revoked or lost. */
    boolean current() {
        return assignment.valid();
    }
}
