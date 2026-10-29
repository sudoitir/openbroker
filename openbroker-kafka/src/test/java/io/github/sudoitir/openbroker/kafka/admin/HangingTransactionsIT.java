package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.Resolution;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Hanging transactions against a broker that lets a transaction last only five seconds and does not abort
 * timed-out ones. A transaction the coordinator has forgotten cannot be made here, so the coordinator's
 * view is replaced by an empty one where the case needs a hanging transaction; the producers, their open
 * transactions and the abort are the cluster's own.
 */
class HangingTransactionsIT {

    private static final String IMAGE = System.getProperty("kafka.image", "apache/kafka:4.4.0");

    private static KafkaContainer broker;
    private static KafkaCluster cluster;
    private static Admin admin;

    @BeforeAll
    static void start() {
        broker = new KafkaContainer(IMAGE.startsWith("confluentinc") ? "apache/kafka:4.4.0" : IMAGE)
                .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "5000")
                .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "3600000")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        broker.start();
        ClusterSpec spec = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, broker.getBootstrapServers())),
                Map.of());
        DriverContext context = new DriverContext() {
            @Override
            public ConnectOptions options() {
                return ConnectOptions.defaults();
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {
                // Nothing is shared in this test.
            }
        };
        cluster = new KafkaCluster(spec, CredentialSource.none(), context);
        admin = Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers()));
    }

    @AfterAll
    static void stop() {
        admin.close();
        cluster.close();
        broker.stop();
    }

    private static Context context() {
        Deadline deadline =
                Deadline.after(Duration.ofSeconds(30), cluster.options().clock());
        return new Context(
                cluster,
                new Topology(cluster.options().clock(), () -> Duration.ofSeconds(10)),
                admin,
                new Await(deadline, cluster.options().clock(), NodeId.of("cluster")),
                Instant.now(),
                null);
    }

    private static KafkaProducer<byte[], byte[]> producer(String transactionalId) {
        return new KafkaProducer<>(
                Map.of(
                        "bootstrap.servers",
                        broker.getBootstrapServers(),
                        "transactional.id",
                        transactionalId,
                        "transaction.timeout.ms",
                        2000),
                new ByteArraySerializer(),
                new ByteArraySerializer());
    }

    /** Leaves a transaction of {@code records} records open, and lets it age past the broker's longest. */
    private static TopicPartition openTransaction(String topic, int records) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        KafkaProducer<byte[], byte[]> producer = producer("hang-" + topic);
        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < records; i++) {
            producer.send(new ProducerRecord<>(topic, 0, null, ("v" + i).getBytes()))
                    .get();
        }
        TopicPartition partition = new TopicPartition(topic, 0);
        awaitAged(partition);
        return partition;
    }

    /** Waits until the producer's last write is older than the broker's longest transaction. */
    private static void awaitAged(TopicPartition partition) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < until) {
            long newest =
                    admin
                            .describeProducers(List.of(partition))
                            .partitionResult(partition)
                            .get()
                            .activeProducers()
                            .stream()
                            .mapToLong(p -> p.lastTimestamp())
                            .max()
                            .orElse(Long.MAX_VALUE);
            if (System.currentTimeMillis() - newest > 5000) {
                return;
            }
            LockSupport.parkNanos(Duration.ofMillis(200).toNanos());
        }
        throw new AssertionError("The transaction never aged past the broker's longest");
    }

    private static long readableByAReadCommittedConsumer(TopicPartition partition) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                Map.of(
                        "bootstrap.servers",
                        broker.getBootstrapServers(),
                        "group.id",
                        "reader-" + partition.topic(),
                        "isolation.level",
                        "read_committed",
                        "auto.offset.reset",
                        "earliest"),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer())) {
            consumer.assign(Set.of(partition));
            ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofSeconds(3));
            return polled.count();
        }
    }

    @Test
    void aTransactionTheCoordinatorStillListsIsNotHanging() throws Exception {
        TopicPartition partition = openTransaction("coordinator-knows", 3);

        assertThat(HangingTransactions.hanging(context(), List.of(partition))).isEmpty();
    }

    @Test
    void anOpenTransactionTheCoordinatorForgotIsFoundDescribedAndAborted() throws Exception {
        TopicPartition partition = openTransaction("coordinator-forgot", 4);
        Context context = context();

        List<HangingTransactions.Open> found =
                HangingTransactions.hanging(context, List.of(partition), ids -> Set.of());

        assertThat(found).singleElement().satisfies(o -> {
            assertThat(o.partition()).isEqualTo(partition);
            assertThat(o.producer().currentTransactionStartOffset()).isPresent();
        });
        HangingTransactions handler = new HangingTransactions(KafkaKinds.base().stream()
                .filter(k -> k.id().equals(KafkaKinds.HANGING_TRANSACTION))
                .findFirst()
                .orElseThrow());
        ResourceId id =
                HangingTransactions.id(partition, found.getFirst().producer().producerId());
        assertThat(readableByAReadCommittedConsumer(partition)).isZero();

        handler.execute(context, id, new Operation.Resolve(Resolution.ROLLBACK), Scope.whole());

        assertThat(HangingTransactions.hanging(context(), List.of(partition), ids -> Set.of()))
                .isEmpty();
        assertThatThrownBy(
                        () -> handler.execute(context(), id, new Operation.Resolve(Resolution.ROLLBACK), Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
    }
}
