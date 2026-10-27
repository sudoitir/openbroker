package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Precision;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Share groups on a topic, on a cluster that runs them in production. */
class KafkaShareGroupsIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;
    private static Admin admin;

    @BeforeAll
    static void start() {
        assumeTrue(
                !KafkaTestCluster.image().startsWith("apache/kafka:4.0"),
                "Share groups are a production feature from Kafka 4.2");
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
        admin = Kafka.admin(kafka);
    }

    @AfterAll
    static void stop() {
        if (admin != null) {
            admin.close();
            connection.close();
            drivers.close();
        }
    }

    private static ResourceId subscription(String group, String topic) {
        return new ResourceId(KafkaKinds.SHARE_GROUP_TOPIC, ResourcePath.of(group, topic));
    }

    private static String topicWith(int partitions) {
        String topic = Kafka.unique("st");
        connection
                .operations()
                .execute(
                        new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(topic)),
                        new Operation.Create(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, (long) partitions)
                                .build()),
                        Scope.whole())
                .orThrow();
        return topic;
    }

    private static Resource read(String group, String topic) {
        return connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.SHARE_GROUP_TOPIC).where(Map.of("group", group, "topic", topic)))
                .orThrow()
                .items()
                .getFirst();
    }

    private static long start(String group, String topic, int partition) throws Exception {
        return admin.listShareGroupOffsets(Map.of(group, new ListShareGroupOffsetsSpec()))
                .all()
                .get()
                .get(group)
                .get(new TopicPartition(topic, partition))
                .startOffset();
    }

    private static final int PARTITIONS = 1;

    private static boolean alter(String group, Map<TopicPartition, Long> starts) {
        try {
            admin.alterShareGroupOffsets(group, starts).all().get();
            return true;
        } catch (Exception notYet) {
            return false;
        }
    }

    /** A share group that has joined and left, so its state exists and it has no member. */
    private static String emptyGroupOn(String topic, int partitions) {
        String group = Kafka.unique("sg");
        try (KafkaShareConsumer<byte[], byte[]> member = Kafka.shareMember(kafka, group, topic)) {
            assertThat(member.subscription()).contains(topic);
        }
        // A group that has only joined has no start offset yet; give each partition one.
        Map<TopicPartition, Long> starts = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            starts.put(new TopicPartition(topic, p), 0L);
        }
        Kafka.eventually(() -> alter(group, starts), done -> done);
        return group;
    }

    @Test
    void aShareGroupsSubscriptionReportsItsBacklogAsAnEstimate() throws Exception {
        String topic = topicWith(2);
        String group = emptyGroupOn(topic, 2);
        Kafka.produce(kafka, topic, 6);

        Resource resource = Kafka.eventually(
                () -> read(group, topic),
                r -> r.attributes().get(MetricKeys.BACKLOG).orElseThrow().asLong() == 6);

        var backlog = resource.attributes().get(MetricKeys.BACKLOG).orElseThrow();
        assertThat(backlog.precision()).isEqualTo(Precision.ESTIMATE);
        assertThat(backlog.asLong()).isEqualTo(6);
        assertThat(resource.attributes().raw(KafkaKinds.GROUP_STATE)).isPresent();
    }

    @Test
    void purgeMovesTheShareGroupsStartOffsetsToTheEndAndNoOtherGroup() throws Exception {
        String topic = topicWith(1);
        String mine = emptyGroupOn(topic, PARTITIONS);
        String other = emptyGroupOn(topic, PARTITIONS);
        Kafka.produce(kafka, topic, 5);
        ResourceId id = subscription(mine, topic);

        Preview preview = connection.operations().preview(id, Operation.purge(), Scope.whole());
        connection
                .operations()
                .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(preview.etag()))
                .orThrow();

        assertThat(start(mine, topic, 0)).isEqualTo(5);
        assertThat(start(other, topic, 0)).isZero();
    }

    @Test
    void seekMovesAShareGroupToAnOffset() throws Exception {
        String topic = topicWith(1);
        String group = emptyGroupOn(topic, PARTITIONS);
        Kafka.produce(kafka, topic, 5);

        connection
                .operations()
                .execute(
                        subscription(group, topic),
                        new Operation.Seek(new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("0")), 3L))),
                        Scope.whole())
                .orThrow();

        assertThat(start(group, topic, 0)).isEqualTo(3);
    }

    @Test
    void anActiveShareGroupIsNotPurgedAndItsMembersAreNamed() throws Exception {
        String topic = topicWith(1);
        String group = Kafka.unique("sg");
        try (KafkaShareConsumer<byte[], byte[]> member = Kafka.shareMember(kafka, group, topic)) {
            ResourceId id = subscription(group, topic);
            Preview preview = connection.operations().preview(id, Operation.purge(), Scope.whole());
            assertThat(preview.diff()).anySatisfy(c -> assertThat(c.key()).isEqualTo(KafkaKinds.MEMBERS));
            Partial<OperationResult> run = connection.operations().execute(id, Operation.purge(), Scope.whole());

            assertThat(run.failures().values()).singleElement().satisfies(e -> {
                assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY);
            });
            assertThat(member.subscription()).contains(topic);
        }
    }
}
