package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * The retry topics of an explicitly managed, unordered processor: one per backoff step,
 * {@code <topic>.retry-<n>} for the record that failed its {@code n}th attempt. Every record of a
 * topic waits the same time, so the head is always the first due; the headers carry the attempt and
 * the due time, and the processor's consumer pauses a retry partition until its head is due. The
 * failed record's partition keeps moving meanwhile.
 *
 * <p>They are created when the processor opens, with as many partitions as the source topic.
 */
final class RetryTopics implements AutoCloseable {

    private static final Duration SEND_WAIT = Duration.ofSeconds(30);

    private final MessagingEnv env;
    private final Topics topics;
    private final RetryPolicy policy;
    private KafkaProducer<byte[], byte[]> producer;

    RetryTopics(MessagingEnv env, Topics topics, RetryPolicy policy) {
        this.env = env;
        this.topics = topics;
        this.policy = policy;
    }

    /** The name of the retry topic of {@code source} for a record that failed attempt {@code attempt}. */
    static String topic(String source, int attempt) {
        return source + ".retry-" + attempt;
    }

    /** The retry topics of {@code source}: one for each attempt that can be retried. */
    List<String> topicsOf(String source) {
        List<String> all = new ArrayList<>();
        for (int attempt = 1; attempt < policy.maxAttempts(); attempt++) {
            all.add(topic(source, attempt));
        }
        return all;
    }

    /** Makes the retry topics of {@code source} exist, before the group is joined. */
    void ensure(String source) {
        int partitions = topics.requirePartitions(source, Targets.topic(source).resource());
        for (String retry : topicsOf(source)) {
            topics.ensure(retry, partitions, "retry", Targets.topic(retry).resource());
        }
    }

    /**
     * Sends a failed record to the retry topic for its attempt, due after the policy's delay for it.
     *
     * @throws BrokerException when the retry topic did not take it; the record is then still unsettled
     */
    synchronized void release(Delivery delivery) {
        int attempt = delivery.attempt();
        Backoff backoff = policy.backoff();
        long due = System.currentTimeMillis() + backoff.delay(attempt).toMillis();
        ConsumerRecord<byte[], byte[]> source = delivery.record();
        Headers headers = new RecordHeaders();
        for (Header header : source.headers()) {
            if (!header.key().equals(RecordCodec.ATTEMPT) && !header.key().equals(RecordCodec.DUE)) {
                headers.add(header);
            }
        }
        if (source.headers().lastHeader(RecordCodec.ORIGIN_TOPIC) == null) {
            headers.add(RecordCodec.ORIGIN_TOPIC, delivery.originTopic().getBytes(StandardCharsets.UTF_8));
            headers.add(
                    RecordCodec.ORIGIN_PARTITION,
                    Integer.toString(source.partition()).getBytes(StandardCharsets.UTF_8));
            headers.add(
                    RecordCodec.ORIGIN_OFFSET, Long.toString(source.offset()).getBytes(StandardCharsets.UTF_8));
        }
        headers.add(RecordCodec.ATTEMPT, Integer.toString(attempt).getBytes(StandardCharsets.UTF_8));
        headers.add(RecordCodec.DUE, Long.toString(due).getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[], byte[]> retry = new ProducerRecord<>(
                topic(delivery.originTopic(), attempt),
                source.partition(),
                null,
                source.key(),
                source.value(),
                headers);
        try {
            producer().send(retry).get(SEND_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw KafkaFailures.of(e instanceof ExecutionException ? e.getCause() : e, Optional.empty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw KafkaFailures.of(e, Optional.empty());
        }
    }

    private KafkaProducer<byte[], byte[]> producer() {
        if (producer == null) {
            producer = Producers.create(env.cluster(), Guarantee.AT_LEAST_ONCE, null);
        }
        return producer;
    }

    @Override
    public synchronized void close() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(2));
            producer = null;
        }
    }
}
