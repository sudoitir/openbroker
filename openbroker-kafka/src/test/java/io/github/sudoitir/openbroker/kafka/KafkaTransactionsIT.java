package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.operation.Resolution;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Transactional ids: listing them and aborting a transaction that was left open. */
class KafkaTransactionsIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
    }

    @AfterAll
    static void stop() {
        connection.close();
        drivers.close();
    }

    private static KafkaProducer<byte[], byte[]> producer(String transactionalId) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.bootstrapServers());
        config.put("transactional.id", transactionalId);
        config.put("transaction.timeout.ms", 600_000);
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static Resource read(String transactionalId) {
        List<Resource> found = connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.TRANSACTIONAL_ID).named(transactionalId))
                .orThrow()
                .items();
        return found.isEmpty() ? null : found.getFirst();
    }

    @Test
    void anOpenTransactionIsListedWithItsStateAndPartitions() throws Exception {
        String topic = Kafka.unique("tx-topic");
        connection
                .operations()
                .execute(
                        new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(topic)),
                        new Operation.Create(Attributes.empty()),
                        Scope.whole())
                .orThrow();
        String id = Kafka.unique("txid");
        try (KafkaProducer<byte[], byte[]> producer = producer(id)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "x".getBytes(StandardCharsets.UTF_8)))
                    .get();

            Resource open = Kafka.eventually(
                    () -> read(id),
                    r -> r != null
                            && "ongoing"
                                    .equals(r.attributes()
                                            .raw(KafkaKinds.TRANSACTION_STATE)
                                            .orElse("")));

            assertThat(open.attributes().raw(KafkaKinds.TRANSACTION_STATE)).contains("ongoing");
            assertThat(open.attributes()
                            .raw(KafkaKinds.TRANSACTION_PARTITIONS)
                            .orElseThrow()
                            .toString())
                    .contains(topic);
            assertThat(open.attributes().raw(KafkaKinds.PRODUCER_ID)).isPresent();
            producer.abortTransaction();
        }
    }

    @Test
    void aTransactionLeftOpenIsAborted() throws Exception {
        String topic = Kafka.unique("tx-topic");
        connection
                .operations()
                .execute(
                        new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(topic)),
                        new Operation.Create(Attributes.empty()),
                        Scope.whole())
                .orThrow();
        String id = Kafka.unique("txid");
        ResourceId resource = new ResourceId(KafkaKinds.TRANSACTIONAL_ID, ResourcePath.of(id));
        try (KafkaProducer<byte[], byte[]> producer = producer(id)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "x".getBytes(StandardCharsets.UTF_8)))
                    .get();
            Kafka.eventually(
                    () -> read(id),
                    r -> r != null
                            && "ongoing"
                                    .equals(r.attributes()
                                            .raw(KafkaKinds.TRANSACTION_STATE)
                                            .orElse("")));

            Preview preview = connection
                    .operations()
                    .preview(resource, new Operation.Resolve(Resolution.ROLLBACK), Scope.whole());
            assertThat(preview.diff())
                    .singleElement()
                    .satisfies(c -> assertThat(c.after()).contains("aborted"));
            connection
                    .operations()
                    .execute(
                            resource,
                            new Operation.Resolve(Resolution.ROLLBACK),
                            Scope.whole(),
                            Precondition.ifMatch(preview.etag()))
                    .orThrow();

            Resource after = Kafka.eventually(
                    () -> read(id),
                    r -> r != null
                            && !"ongoing"
                                    .equals(r.attributes()
                                            .raw(KafkaKinds.TRANSACTION_STATE)
                                            .orElse("")));
            assertThat(after.attributes().raw(KafkaKinds.TRANSACTION_STATE).orElse(""))
                    .isNotEqualTo("ongoing");
        }
    }

    @Test
    void aTransactionIsNeverCommittedForItsProducer() {
        String id = Kafka.unique("txid");

        Partial<OperationResult> commit = connection
                .operations()
                .execute(
                        new ResourceId(KafkaKinds.TRANSACTIONAL_ID, ResourcePath.of(id)),
                        new Operation.Resolve(Resolution.COMMIT),
                        Scope.whole());

        assertThat(commit.failures().values()).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.getMessage()).contains("not commit it");
        });
    }
}
