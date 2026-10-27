package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Truncate: records removed for every group, with each group's loss named first. */
class KafkaTruncateIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;
    private static Admin admin;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
        admin = Kafka.admin(kafka);
    }

    @AfterAll
    static void stop() {
        admin.close();
        connection.close();
        drivers.close();
    }

    private static ResourceId topic(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static String topicWith(int partitions, Attributes extra) {
        String name = Kafka.unique("trunc");
        connection
                .operations()
                .execute(
                        topic(name),
                        new Operation.Create(Attributes.builder()
                                .putAll(extra)
                                .putRaw(KafkaKinds.PARTITIONS, (long) partitions)
                                .build()),
                        Scope.whole())
                .orThrow();
        return name;
    }

    private static void commit(String group, String topic, long... offsets) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> at = new HashMap<>();
        for (int p = 0; p < offsets.length; p++) {
            at.put(new TopicPartition(topic, p), new OffsetAndMetadata(offsets[p]));
        }
        admin.alterConsumerGroupOffsets(group, at).all().get();
    }

    private static long logStart(String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        return admin.listOffsets(Map.of(tp, OffsetSpec.earliest()))
                .all()
                .get()
                .get(tp)
                .offset();
    }

    @Test
    void thePreviewNamesEveryGroupThatLosesUnreadRecordsAndHowMany() throws Exception {
        String topic = topicWith(2, Attributes.empty());
        Kafka.produce(kafka, topic, 10);
        String behind = Kafka.unique("behind");
        String partway = Kafka.unique("partway");
        String caughtUp = Kafka.unique("caught-up");
        commit(behind, topic, 0, 0);
        commit(partway, topic, 3, 0);
        commit(caughtUp, topic, 5, 5);

        Preview preview =
                connection.operations().preview(topic(topic), new Operation.Truncate(Position.latest()), Scope.whole());

        Map<String, Object> lost = new HashMap<>();
        for (AttributeChange change : preview.diff()) {
            if (change.key().startsWith("kafka.unread_lost.")) {
                lost.put(
                        change.key().substring("kafka.unread_lost.".length()),
                        change.after().orElseThrow());
            }
        }
        assertThat(lost).containsEntry(behind, 10L).containsEntry(partway, 7L).doesNotContainKey(caughtUp);
        assertThat(preview.estimate().count()).isEqualTo(Count.estimate(10));
        assertThat(preview.diff()).anySatisfy(c -> assertThat(c.key()).isEqualTo(KafkaKinds.LOG_START_OFFSET + ".0"));
    }

    @Test
    void truncatingRemovesTheRecordsForAllGroups() throws Exception {
        String topic = topicWith(2, Attributes.empty());
        Kafka.produce(kafka, topic, 10);
        ResourceId id = topic(topic);
        Operation truncate = new Operation.Truncate(Position.latest());

        Preview preview = connection.operations().preview(id, truncate, Scope.whole());
        OperationResult result = connection
                .operations()
                .execute(id, truncate, Scope.whole(), Precondition.ifMatch(preview.etag()))
                .orThrow();

        assertThat(result.affected()).hasValue(10);
        assertThat(logStart(topic, 0)).isEqualTo(5);
        assertThat(logStart(topic, 1)).isEqualTo(5);
        assertThat(connection.operations().preview(id, truncate, Scope.whole()).etag())
                .isNotEqualTo(preview.etag());
    }

    @Test
    void aTruncationThatAlreadyRanIsRefusedByItsEtag() throws Exception {
        String topic = topicWith(1, Attributes.empty());
        Kafka.produce(kafka, topic, 6);
        ResourceId id = topic(topic);
        Operation latest = new Operation.Truncate(Position.latest());
        Preview preview = connection.operations().preview(id, latest, Scope.whole());
        connection.operations().execute(id, latest, Scope.whole()).orThrow();

        Partial<OperationResult> again =
                connection.operations().execute(id, latest, Scope.whole(), Precondition.ifMatch(preview.etag()));

        assertThat(again.failures().values()).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
        });
    }

    @Test
    void aPartitionIsTruncatedToAnOffsetAlone() throws Exception {
        String topic = topicWith(2, Attributes.empty());
        Kafka.produce(kafka, topic, 10);
        ResourceId partition = new ResourceId(KafkaKinds.PARTITION, ResourcePath.of(topic, "1"));

        connection
                .operations()
                .execute(
                        partition,
                        new Operation.Truncate(
                                new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("1")), 3L))),
                        Scope.whole())
                .orThrow();

        assertThat(logStart(topic, 0)).isZero();
        assertThat(logStart(topic, 1)).isEqualTo(3);
    }

    @Test
    void truncatingToAnEarlierOffsetThanTheLogStartDoesNothing() throws Exception {
        String topic = topicWith(1, Attributes.empty());
        Kafka.produce(kafka, topic, 4);

        OperationResult result = connection
                .operations()
                .execute(topic(topic), new Operation.Truncate(Position.earliest()), Scope.whole())
                .orThrow();

        assertThat(result.alreadyInState()).isTrue();
    }

    @Test
    void aCompactedTopicIsRefusedBeforeKafkaIs() throws Exception {
        String topic = topicWith(
                1,
                Attributes.builder()
                        .putRaw(KafkaKinds.CONFIG_PREFIX + "cleanup.policy", "compact")
                        .build());
        Kafka.produce(kafka, topic, 3);

        assertThatThrownBy(() -> connection
                        .operations()
                        .preview(topic(topic), new Operation.Truncate(Position.latest()), Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.getMessage()).contains("cleanup.policy=compact");
                });
        assertThat(logStart(topic, 0)).isZero();
    }

    @Test
    void aTopicThatCompactsAndDeletesCanBeTruncated() throws Exception {
        String topic = topicWith(
                1,
                Attributes.builder()
                        .putRaw(KafkaKinds.CONFIG_PREFIX + "cleanup.policy", "compact,delete")
                        .build());
        Kafka.produce(kafka, topic, 3);

        connection
                .operations()
                .execute(topic(topic), new Operation.Truncate(Position.latest()), Scope.whole())
                .orThrow();

        assertThat(logStart(topic, 0)).isEqualTo(3);
    }
}
