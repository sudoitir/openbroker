package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Group subscriptions: their lag, and moving where a group stands. */
class KafkaSubscriptionsIT {

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

    private static ResourceId subscription(String group, String topic) {
        return new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(group, topic));
    }

    private static String topicWith(int partitions) {
        String topic = Kafka.unique("t");
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

    private static Resource read(ResourceId id) {
        return connection
                .resources()
                .read(ResourceQuery.of(id.kind())
                        .where(Map.of("group", id.path().segments().get(0), "topic", id.name())))
                .orThrow()
                .items()
                .getFirst();
    }

    private static long committed(String group, String topic, int partition) throws Exception {
        return admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .get()
                .get(new TopicPartition(topic, partition))
                .offset();
    }

    private static Map<TopicPartition, OffsetAndMetadata> at(String topic, long... offsets) {
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        for (int p = 0; p < offsets.length; p++) {
            if (offsets[p] >= 0) {
                out.put(new TopicPartition(topic, p), new OffsetAndMetadata(offsets[p]));
            }
        }
        return out;
    }

    @Test
    void creatingASubscriptionMakesTheGroupAndItsTopic() {
        String group = Kafka.unique("g");
        String topic = Kafka.unique("made");
        ResourceId id = subscription(group, topic);

        connection
                .operations()
                .execute(
                        id,
                        new Operation.Create(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 2L)
                                .build()),
                        Scope.whole())
                .orThrow();

        Resource created = read(id);
        assertThat(created.attributes().get(MetricKeys.BACKLOG).orElseThrow().asLong())
                .isZero();
        assertThat(connection
                        .operations()
                        .execute(id, new Operation.Create(Attributes.empty()), Scope.whole())
                        .orThrow()
                        .alreadyInState())
                .isTrue();
        connection.operations().execute(id, Operation.delete(), Scope.whole()).orThrow();
        assertThat(connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.GROUP_TOPIC).where(Map.of("group", group)))
                        .orThrow()
                        .items())
                .isEmpty();
    }

    @Test
    void lagIsAnEstimateWithEachPartitionsShareAndAnUnknownPartitionSaysWhy() throws Exception {
        String topic = topicWith(6);
        Kafka.produce(kafka, topic, 12);
        String group = Kafka.unique("g");
        // The group has committed on five partitions and never on the sixth.
        admin.alterConsumerGroupOffsets(group, at(topic, 0, 0, 0, 0, 0, 0, -1))
                .all()
                .get();
        admin.alterConsumerGroupOffsets(group, at(topic, 0, 0, 0, 0, 0, 0))
                .all()
                .get();
        admin.deleteConsumerGroupOffsets(group, Set.of(new TopicPartition(topic, 5)))
                .all()
                .get();

        Resource resource = read(subscription(group, topic));

        MetricValue backlog = resource.attributes().get(MetricKeys.BACKLOG).orElseThrow();
        assertThat(backlog.precision()).isEqualTo(Precision.ESTIMATE);
        assertThat(backlog.breakdown()).hasSize(5);
        assertThat(backlog.breakdown().keySet()).doesNotContain(Shard.partition(PartitionId.of("5")));
        assertThat(backlog.breakdown().values().stream()
                        .mapToDouble(Double::doubleValue)
                        .sum())
                .isEqualTo(backlog.value());
        assertThat(resource.attributes().raw(KafkaKinds.LAG_UNAVAILABLE).orElseThrow())
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsKey(5)
                .extractingByKey(5)
                .asString()
                .contains("no committed offset");
        assertThat(resource.conditions()).anySatisfy(c -> {
            assertThat(c.type()).isEqualTo(KafkaKinds.BACKLOG_ESTIMATE);
            assertThat(c.message()).contains("transaction markers").contains("compaction");
        });
    }

    @Test
    void aCommittedGroupIsLaggingByWhatItHasNotRead() throws Exception {
        String topic = topicWith(1);
        Kafka.produce(kafka, topic, 10);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 4)).all().get();

        assertThat(read(subscription(group, topic))
                        .attributes()
                        .get(MetricKeys.BACKLOG)
                        .orElseThrow()
                        .asLong())
                .isEqualTo(6);
    }

    @Test
    void aClassicApplicationGroupIsListedReadOnlyAndHasNoSubscription() throws Exception {
        String topic = topicWith(1);
        String group = Kafka.unique("classic");
        try (KafkaConsumer<byte[], byte[]> member = Kafka.member(kafka, group, topic, "classic")) {
            member.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(0)));
        }

        Resource listed = connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.GROUP).named(group))
                .orThrow()
                .items()
                .getFirst();

        assertThat(listed.attributes().raw(KafkaKinds.GROUP_TYPE)).contains("classic");
        assertThat(listed.attributes()
                        .raw(KafkaKinds.READ_ONLY_REASON)
                        .orElseThrow()
                        .toString())
                .contains("belong to the application");
        assertThat(connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.GROUP_TOPIC).where(Map.of("group", group)))
                        .orThrow()
                        .items())
                .isEmpty();
        Partial<OperationResult> purge = connection
                .operations()
                .execute(new ResourceId(KafkaKinds.GROUP, ResourcePath.of(group)), Operation.purge(), Scope.whole());
        assertThat(purge.failures().values()).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.getMessage()).contains("belong to the application");
        });
    }

    @Test
    void purgingAnActiveGroupNamesItsMembersAndChangesNothing() throws Exception {
        String topic = topicWith(2);
        Kafka.produce(kafka, topic, 6);
        String group = Kafka.unique("active");
        try (KafkaConsumer<byte[], byte[]> member = Kafka.member(kafka, group, topic, "consumer")) {
            member.commitSync(Map.of(
                    new TopicPartition(topic, 0),
                    new OffsetAndMetadata(0),
                    new TopicPartition(topic, 1),
                    new OffsetAndMetadata(0)));
            ResourceId id = subscription(group, topic);

            Preview preview = connection.operations().preview(id, Operation.purge(), Scope.whole());
            assertThat(preview.diff()).anySatisfy(change -> {
                assertThat(change.key()).isEqualTo(KafkaKinds.MEMBERS);
                assertThat(change.before().orElseThrow().toString()).contains("consumer-");
            });

            Partial<OperationResult> run = connection
                    .operations()
                    .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(preview.etag()));
            assertThat(run.failures().values()).singleElement().satisfies(e -> {
                assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY);
            });
            assertThat(committed(group, topic, 0)).isZero();
            assertThat(committed(group, topic, 1)).isZero();
        }
    }

    @Test
    void purgeMovesOnlyThatGroupToTheEnd() throws Exception {
        String topic = topicWith(2);
        Kafka.produce(kafka, topic, 8);
        String mine = Kafka.unique("mine");
        String other = Kafka.unique("other");
        admin.alterConsumerGroupOffsets(mine, at(topic, 0, 0)).all().get();
        admin.alterConsumerGroupOffsets(other, at(topic, 0, 0)).all().get();

        Preview preview = connection.operations().preview(subscription(mine, topic), Operation.purge(), Scope.whole());
        assertThat(preview.estimate().count()).isEqualTo(Count.estimate(8));
        connection
                .operations()
                .execute(
                        subscription(mine, topic),
                        Operation.purge(),
                        Scope.whole(),
                        Precondition.ifMatch(preview.etag()))
                .orThrow();

        assertThat(read(subscription(mine, topic))
                        .attributes()
                        .get(MetricKeys.BACKLOG)
                        .orElseThrow()
                        .asLong())
                .isZero();
        assertThat(read(subscription(other, topic))
                        .attributes()
                        .get(MetricKeys.BACKLOG)
                        .orElseThrow()
                        .asLong())
                .isEqualTo(8);
    }

    @Test
    void anEtagRefusesAGroupThatMovedSinceThePreview() throws Exception {
        String topic = topicWith(1);
        Kafka.produce(kafka, topic, 5);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 0)).all().get();
        ResourceId id = subscription(group, topic);
        Preview preview = connection.operations().preview(id, Operation.purge(), Scope.whole());

        admin.alterConsumerGroupOffsets(group, at(topic, 3)).all().get();
        Partial<OperationResult> run = connection
                .operations()
                .execute(id, Operation.purge(), Scope.whole(), Precondition.ifMatch(preview.etag()));

        assertThat(run.failures().values()).singleElement().satisfies(e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
        });
        assertThat(committed(group, topic, 0)).isEqualTo(3);
    }

    @Test
    void seekMovesToEarliestLatestAnOffsetAndATime() throws Exception {
        String topic = topicWith(1);
        Kafka.produce(kafka, topic, 4);
        Instant middle = Instant.now();
        Thread.sleep(50);
        Kafka.produce(kafka, topic, 4);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 2)).all().get();
        ResourceId id = subscription(group, topic);

        seek(id, Position.latest());
        assertThat(committed(group, topic, 0)).isEqualTo(8);
        seek(id, Position.earliest());
        assertThat(committed(group, topic, 0)).isZero();
        seek(id, new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("0")), 5L)));
        assertThat(committed(group, topic, 0)).isEqualTo(5);
        seek(id, new Position.AtTime(middle));
        assertThat(committed(group, topic, 0)).isEqualTo(4);
        seek(id, new Position.AtTime(Instant.now().plusSeconds(3600)));
        assertThat(committed(group, topic, 0)).isEqualTo(8);
    }

    @Test
    void anOffsetOutsideTheLogIsRefused() throws Exception {
        String topic = topicWith(1);
        Kafka.produce(kafka, topic, 2);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 0)).all().get();

        Partial<OperationResult> run = connection
                .operations()
                .execute(
                        subscription(group, topic),
                        new Operation.Seek(new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("0")), 99L))),
                        Scope.whole());

        assertThat(run.failures().values())
                .singleElement()
                .satisfies(e -> assertThat(e.status()).isEqualTo(Status.OUT_OF_RANGE));
    }

    @Test
    void aSnapshotOrCursorNamesNoPlaceInAKafkaLog() throws Exception {
        String topic = topicWith(1);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 0)).all().get();

        assertThat(connection
                        .operations()
                        .execute(
                                subscription(group, topic),
                                new Operation.Seek(new Position.AtSnapshot(subscription(group, topic))),
                                Scope.whole())
                        .failures()
                        .values())
                .singleElement()
                .satisfies(e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }

    private static void seek(ResourceId id, Position position) {
        connection
                .operations()
                .execute(id, new Operation.Seek(position), Scope.whole())
                .orThrow();
    }

    @Test
    void aSeekCanBeScopedToOnePartition() throws Exception {
        String topic = topicWith(2);
        Kafka.produce(kafka, topic, 6);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(topic, 0, 0)).all().get();

        connection
                .operations()
                .execute(
                        subscription(group, topic),
                        new Operation.Seek(Position.latest()),
                        Scope.shards(Set.of(Shard.partition(PartitionId.of("1")))))
                .orThrow();

        assertThat(committed(group, topic, 0)).isZero();
        assertThat(committed(group, topic, 1)).isPositive();
    }

    @Test
    void deletingASubscriptionForgetsOnlyItsTopic() throws Exception {
        String one = topicWith(1);
        String two = topicWith(1);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(group, at(one, 0)).all().get();
        admin.alterConsumerGroupOffsets(group, at(two, 0)).all().get();

        connection
                .operations()
                .execute(subscription(group, one), Operation.delete(), Scope.whole())
                .orThrow();

        assertThat(connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.GROUP_TOPIC).where(Map.of("group", group)))
                        .orThrow()
                        .items())
                .extracting(Resource::id)
                .containsExactly(subscription(group, two));
    }

    @Test
    void aSubscriptionOfAnActiveGroupCountsItsConsumers() throws Exception {
        String topic = topicWith(1);
        String group = Kafka.unique("g");
        try (KafkaConsumer<byte[], byte[]> member = Kafka.member(kafka, group, topic, "consumer")) {
            member.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(0)));

            Resource resource = read(subscription(group, topic));

            assertThat(resource.attributes().get(MetricKeys.CONSUMER_COUNT).map(MetricValue::asLong))
                    .contains(1L);
            assertThat(resource.attributes().raw(KafkaKinds.GROUP_STATE)).contains("stable");
        }
    }
}
