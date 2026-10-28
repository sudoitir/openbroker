package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Helpers the integration tests share. */
final class Kafka {

    private Kafka() {}

    static ClusterConnection connect(BrokerDrivers drivers, KafkaTestCluster kafka) {
        return connect(drivers, kafka, Map.of());
    }

    static ClusterConnection connect(BrokerDrivers drivers, KafkaTestCluster kafka, Map<String, String> options) {
        return drivers.connect(new ConnectRequest(
                KafkaDriver.SYSTEM,
                kafka.spec().withOptions(options),
                CredentialSource.none(),
                ConnectOptions.defaults()));
    }

    static Admin admin(KafkaTestCluster kafka) {
        return Admin.create(Map.of("bootstrap.servers", kafka.bootstrapServers()));
    }

    /**
     * The value once {@code settled} accepts it, or the last one after ten seconds. Brokers serve
     * metadata from their own copies, which trail a change by a moment, so a read that follows a change
     * made through another broker may need a second look.
     */
    static <T> T eventually(Supplier<T> read, Predicate<T> settled) {
        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        T value = read.get();
        while (!settled.test(value) && System.nanoTime() < until) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return value;
            }
            value = read.get();
        }
        return value;
    }

    /** A name no other test uses. */
    static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Records written straight to a topic, one per partition in turn. */
    static void produce(KafkaTestCluster kafka, String topic, int records) {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                Map.of("bootstrap.servers", kafka.bootstrapServers()),
                new ByteArraySerializer(),
                new ByteArraySerializer())) {
            int partitions = producer.partitionsFor(topic).size();
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>(
                                topic,
                                i % partitions,
                                ("k" + i).getBytes(StandardCharsets.UTF_8),
                                ("v" + i).getBytes(StandardCharsets.UTF_8)))
                        .get();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A consumer that has joined {@code group} on {@code topic} with the given protocol, and is
     * assigned its partitions; {@code consumer} falls back to {@code classic} on a cluster that lacks
     * the consumer protocol. Close it to leave the group.
     */
    static KafkaConsumer<byte[], byte[]> member(KafkaTestCluster kafka, String group, String topic, String protocol) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.bootstrapServers());
        config.put("group.id", group);
        config.put(
                "group.protocol",
                protocol.equals("consumer") && !kafka.supports("group.version", 1) ? "classic" : protocol);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");
        KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(topic));
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (consumer.assignment().isEmpty() && System.nanoTime() < until) {
            consumer.poll(Duration.ofMillis(200));
        }
        return consumer;
    }

    /**
     * A share consumer that has joined {@code group} on {@code topic} and been assigned its partitions.
     * Close it to leave the group.
     */
    static KafkaShareConsumer<byte[], byte[]> shareMember(KafkaTestCluster kafka, String group, String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.bootstrapServers());
        config.put("group.id", group);
        KafkaShareConsumer<byte[], byte[]> consumer =
                new KafkaShareConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(topic));
        try (Admin admin = admin(kafka)) {
            long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < until) {
                consumer.poll(Duration.ofMillis(200));
                try {
                    boolean assigned =
                            admin.describeShareGroups(List.of(group)).all().get().get(group).members().stream()
                                    .anyMatch(m ->
                                            !m.assignment().topicPartitions().isEmpty());
                    if (assigned) {
                        return consumer;
                    }
                } catch (Exception notYet) {
                    // The group does not exist until the first member has joined.
                }
            }
        }
        throw new IllegalStateException("The share consumer was never assigned a partition of " + topic);
    }
}
