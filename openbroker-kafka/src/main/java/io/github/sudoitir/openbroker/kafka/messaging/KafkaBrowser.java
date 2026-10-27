package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * A receiver that only reads. It joins no group and commits nothing: it takes the topic's partitions
 * itself and starts each at the group's committed offset (the start of the log where the group has
 * none), so it shows what the subscription still has to deliver. Settling is a no-op, and it reads
 * committed records only. Single-owner: the owner's thread polls.
 */
final class KafkaBrowser implements Receiver {

    private static final Duration POLL = Duration.ofMillis(100);

    private final MessagingEnv env;
    private final Subscriptions.GroupTopic subscription;
    private final Optional<Instant> producedAtOrBefore;
    private final String id = UUID.randomUUID().toString();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final ArrayDeque<ConsumerRecord<byte[], byte[]>> ready = new ArrayDeque<>();
    private volatile boolean closed;

    KafkaBrowser(MessagingEnv env, Subscriptions.GroupTopic subscription, ReceiverOptions options) {
        this.env = env;
        this.subscription = subscription;
        this.producedAtOrBefore = options.producedAtOrBefore();
        KafkaCluster cluster = env.cluster();
        Map<String, Object> config = new HashMap<>(cluster.clientConfig(Plane.DATA));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, config.get(ConsumerConfig.CLIENT_ID_CONFIG) + "-browse-" + id);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.consumer = new KafkaConsumer<>(config);
        try {
            List<TopicPartition> partitions = consumer.partitionsFor(subscription.topic()).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            Map<TopicPartition, OffsetAndMetadata> committed = committed(partitions);
            List<TopicPartition> fresh = new ArrayList<>();
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata offset = committed.get(partition);
                if (offset == null) {
                    fresh.add(partition);
                } else {
                    consumer.seek(partition, offset.offset());
                }
            }
            consumer.seekToBeginning(fresh);
        } catch (RuntimeException e) {
            consumer.close(Duration.ofSeconds(1));
            throw e;
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> committed(List<TopicPartition> partitions) {
        KafkaCluster cluster = env.cluster();
        Map<TopicPartition, OffsetAndMetadata> found = cluster.call(
                "group.offsets",
                Optional.of(subscription.id()),
                Deadlines.current(cluster.options()),
                (admin, await) -> await.get(
                        admin.listConsumerGroupOffsets(subscription.group()).partitionsToOffsetAndMetadata()));
        Map<TopicPartition, OffsetAndMetadata> mine = new HashMap<>();
        partitions.forEach(p -> {
            if (found.containsKey(p)) {
                mine.put(p, found.get(p));
            }
        });
        return mine;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : LinkState.OPEN;
    }

    @Override
    public List<Message> receive(int max, Duration timeout) {
        long end = System.nanoTime() + timeout.toNanos();
        List<Message> out = new ArrayList<>();
        try {
            while (out.isEmpty()) {
                if (ready.isEmpty()) {
                    consumer.poll(POLL).forEach(ready::add);
                }
                while (!ready.isEmpty() && out.size() < max) {
                    ConsumerRecord<byte[], byte[]> record = ready.poll();
                    if (producedAtOrBefore.isEmpty()
                            || record.timestamp() <= producedAtOrBefore.get().toEpochMilli()) {
                        out.add(RecordCodec.message(subscription.id(), record, Long.MAX_VALUE, 1));
                    }
                }
                if (out.isEmpty() && System.nanoTime() - end >= 0) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            throw KafkaFailures.of(e, Optional.of(subscription.id()));
        }
        return out;
    }

    /** Browsing leaves every message where it is. */
    @Override
    public void settle(Message message, Disposition disposition) {}

    @Override
    public Sender sender(SendTarget target) {
        throw KafkaFailures.precondition("Only a TRANSACTED receiver has a transaction to join.");
    }

    @Override
    public void commit() {
        throw KafkaFailures.precondition("A browsing receiver has nothing to commit.");
    }

    @Override
    public void rollback() {
        throw KafkaFailures.precondition("A browsing receiver has nothing to roll back.");
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            consumer.close(Duration.ofSeconds(2));
            env.forget(this);
        }
    }
}
