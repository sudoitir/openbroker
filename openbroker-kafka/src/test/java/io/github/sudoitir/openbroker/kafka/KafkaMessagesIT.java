package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.MessageAction;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.operation.Operation;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Sending, browsing and counting messages by hand. */
class KafkaMessagesIT {

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

    private static ResourceId topicId(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static String topicWith(int partitions) {
        String name = Kafka.unique("m");
        connection
                .operations()
                .execute(
                        topicId(name),
                        new Operation.Create(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, (long) partitions)
                                .build()),
                        Scope.whole())
                .orThrow();
        return name;
    }

    private static BrowseRequest all(int limit) {
        return new BrowseRequest(Position.earliest(), limit, Optional.empty(), 1024);
    }

    private static List<String> bodies(BrowsePage page) {
        return page.messages().stream()
                .map(m -> m.body().bytes().orElseThrow().asUtf8())
                .toList();
    }

    @Test
    void aSentMessageIsBrowsedWithItsHeadersAndTypedProperties() {
        String topic = topicWith(1);
        ResourceId id = topicId(topic);
        SendReceipt receipt = connection
                .messages()
                .send(
                        SendTarget.destination(id),
                        new OutboundMessage(
                                MessageHeaders.builder()
                                        .id("m-1")
                                        .correlationId("c-1")
                                        .type("t.1")
                                        .build(),
                                Map.of(
                                        "text",
                                        PropertyValue.of("hello"),
                                        "number",
                                        PropertyValue.of(42L),
                                        "flag",
                                        PropertyValue.of(true)),
                                MessageBody.of("body".getBytes(StandardCharsets.UTF_8)),
                                Map.of()));

        BrowsePage page = connection.messages().browse(id, Scope.whole(), all(10));

        assertThat(receipt.ref().orElseThrow().position()).isEqualTo("0");
        assertThat(page.messages()).singleElement().satisfies(m -> {
            assertThat(m.headers().id()).contains("m-1");
            assertThat(m.headers().correlationId()).contains("c-1");
            assertThat(m.headers().type()).contains("t.1");
            assertThat(m.properties())
                    .containsEntry("text", PropertyValue.of("hello"))
                    .containsEntry("number", PropertyValue.of(42L))
                    .containsEntry("flag", PropertyValue.of(true));
            assertThat(m.body().bytes().orElseThrow().asUtf8()).isEqualTo("body");
            assertThat(m.extension("kafka").raw("partition")).contains(0L);
        });
    }

    @Test
    void aBrowseFromAnOffsetAFollowedCursorAndATimeAllReadTheSameLog() {
        String topic = topicWith(1);
        ResourceId id = topicId(topic);
        Kafka.produce(kafka, topic, 4);
        Instant middle = Instant.now();
        sleep(50);
        Kafka.produce(kafka, topic, 4);

        BrowsePage first = connection.messages().browse(id, Scope.whole(), all(3));
        assertThat(first.messages()).hasSize(3);
        assertThat(first.next()).isPresent();
        List<String> seen = new ArrayList<>(bodies(first));
        BrowsePage page = first;
        while (page.next().isPresent()) {
            page = connection
                    .messages()
                    .browse(
                            id,
                            Scope.whole(),
                            new BrowseRequest(Position.after(page.next().get()), 3, Optional.empty(), 1024));
            seen.addAll(bodies(page));
        }
        // Each batch numbers its records from 0, so the log reads 0 to 3 twice.
        assertThat(seen).containsExactly("v0", "v1", "v2", "v3", "v0", "v1", "v2", "v3");

        BrowsePage fromOffset = connection
                .messages()
                .browse(
                        id,
                        Scope.whole(),
                        new BrowseRequest(
                                new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("0")), 5L)),
                                10,
                                Optional.empty(),
                                1024));
        assertThat(bodies(fromOffset)).containsExactly("v1", "v2", "v3");

        BrowsePage fromTime = connection
                .messages()
                .browse(id, Scope.whole(), new BrowseRequest(new Position.AtTime(middle), 10, Optional.empty(), 1024));
        assertThat(bodies(fromTime)).containsExactly("v0", "v1", "v2", "v3");
        assertThat(fromTime.messages().getFirst().ref().position()).isEqualTo("4");
        assertThat(fromTime.total().count()).isEqualTo(Count.estimate(8));
    }

    @Test
    void aBrowseIsScopedToPartitions() {
        String topic = topicWith(3);
        Kafka.produce(kafka, topic, 9);

        BrowsePage one = connection
                .messages()
                .browse(topicId(topic), Scope.shards(Set.of(Shard.partition(PartitionId.of("1")))), all(10));

        assertThat(one.messages()).hasSize(3);
        assertThat(one.messages())
                .allSatisfy(
                        m -> assertThat(m.extension("kafka").raw("partition")).contains(1L));
    }

    @Test
    void aBrowseJoinsNoGroupAndMovesNoOffset() throws Exception {
        String topic = topicWith(2);
        Kafka.produce(kafka, topic, 6);
        String group = Kafka.unique("watcher");
        admin.alterConsumerGroupOffsets(
                        group,
                        Map.of(
                                new TopicPartition(topic, 0),
                                new OffsetAndMetadata(0),
                                new TopicPartition(topic, 1),
                                new OffsetAndMetadata(0)))
                .all()
                .get();
        Set<String> before = groups();

        ResourceId subscription = new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(group, topic));
        BrowsePage page = connection.messages().browse(subscription, Scope.whole(), all(10));

        assertThat(page.messages()).hasSize(6);
        assertThat(groups()).isEqualTo(before);
        assertThat(admin.describeConsumerGroups(List.of(group))
                        .all()
                        .get()
                        .get(group)
                        .members())
                .isEmpty();
        assertThat(admin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .values())
                .allSatisfy(o -> assertThat(o.offset()).isZero());
    }

    private static Set<String> groups() throws Exception {
        return admin.listGroups().all().get().stream().map(g -> g.groupId()).collect(Collectors.toSet());
    }

    @Test
    void aBrowseShowsOnlyWhatIsCommitted() throws Exception {
        String topic = topicWith(1);
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.bootstrapServers());
        config.put("transactional.id", Kafka.unique("tx"));
        try (KafkaProducer<byte[], byte[]> tx =
                new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer())) {
            tx.initTransactions();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, "committed-1".getBytes(StandardCharsets.UTF_8)))
                    .get();
            tx.commitTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, "aborted".getBytes(StandardCharsets.UTF_8)))
                    .get();
            tx.abortTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, "committed-2".getBytes(StandardCharsets.UTF_8)))
                    .get();
            tx.commitTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, "open".getBytes(StandardCharsets.UTF_8)))
                    .get();

            BrowsePage page = connection.messages().browse(topicId(topic), Scope.whole(), all(10));

            assertThat(bodies(page)).containsExactly("committed-1", "committed-2");
            tx.abortTransaction();
        }
    }

    @Test
    void aBrowseIsBoundedByCountAndByBodyBytes() {
        String topic = topicWith(1);
        ResourceId id = topicId(topic);
        connection.messages().send(SendTarget.destination(id), OutboundMessage.of(MessageBody.of(new byte[500])));
        Kafka.produce(kafka, topic, 5);

        BrowsePage page = connection
                .messages()
                .browse(id, Scope.whole(), new BrowseRequest(Position.earliest(), 2, Optional.empty(), 16));

        assertThat(page.messages()).hasSize(2);
        assertThat(page.messages().getFirst().body().size()).isEqualTo(16);
        assertThat(page.messages().getFirst().truncated()).containsExactly(MessagePart.BODY);
        assertThat(page.next()).isPresent();
    }

    @Test
    void aSubscriptionsCountIsItsLagAsAnEstimateAndATopicsIsItsSize() throws Exception {
        String topic = topicWith(2);
        Kafka.produce(kafka, topic, 10);
        String group = Kafka.unique("g");
        admin.alterConsumerGroupOffsets(
                        group,
                        Map.of(
                                new TopicPartition(topic, 0),
                                new OffsetAndMetadata(2),
                                new TopicPartition(topic, 1),
                                new OffsetAndMetadata(5)))
                .all()
                .get();
        ResourceId subscription = new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(group, topic));

        assertThat(connection
                        .messages()
                        .count(subscription, Scope.whole(), Optional.empty())
                        .orThrow()
                        .count())
                .isEqualTo(Count.estimate(3));
        assertThat(connection
                        .messages()
                        .count(topicId(topic), Scope.whole(), Optional.empty())
                        .orThrow()
                        .count())
                .isEqualTo(Count.estimate(10));
        assertThat(connection
                        .messages()
                        .count(
                                new ResourceId(KafkaKinds.PARTITION, ResourcePath.of(topic, "0")),
                                Scope.whole(),
                                Optional.empty())
                        .orThrow()
                        .count())
                .isEqualTo(Count.estimate(5));
    }

    @Test
    void countingOrBrowsingByAFilterIsRefused() {
        String topic = topicWith(1);
        MessageFilter filter = MessageFilter.portable("property.colour = 'red'");

        assertThat(connection
                        .messages()
                        .count(topicId(topic), Scope.whole(), Optional.of(filter))
                        .failures()
                        .values())
                .singleElement()
                .satisfies(e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        assertThatThrownBy(() -> connection
                        .messages()
                        .browse(
                                topicId(topic),
                                Scope.whole(),
                                new BrowseRequest(Position.earliest(), 1, Optional.of(filter), 10)))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }

    @Test
    void movingCopyingAndDeletingASingleMessageIsUnavailableWithTheReason() {
        String topic = topicWith(1);

        assertThatThrownBy(() -> connection
                        .messages()
                        .bulk(
                                topicId(topic),
                                Scope.whole(),
                                new Selection.All(Optional.empty()),
                                new MessageAction.Delete()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                    assertThat(e.getMessage()).contains("append-only");
                });
        var access = connection.access().probe();
        for (String id : List.of("messages.move", "messages.copy", "messages.delete")) {
            Assessment assessment = access.of(new Capability(id));
            assertThat(assessment.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
            assertThat(assessment.reason()).contains("append-only");
        }
    }

    @Test
    void aSendToATopicThatDoesNotExistDoesNotMakeIt() {
        String topic = Kafka.unique("missing");

        assertThatThrownBy(() -> connection
                        .messages()
                        .send(SendTarget.destination(topicId(topic)), OutboundMessage.of(MessageBody.empty())))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.NOT_FOUND));
        assertThat(connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.TOPIC).named(topic))
                        .orThrow()
                        .items())
                .isEmpty();
    }

    @Test
    void aSendNamesTheRecordItWrote() {
        String topic = topicWith(2);
        SendReceipt receipt = connection
                .messages()
                .send(
                        new SendTarget.ToDestination(
                                topicId(topic), Optional.empty(), Optional.of(PartitionId.of("1"))),
                        OutboundMessage.of(MessageBody.of(new byte[] {1})));

        assertThat(receipt.ref().orElseThrow().shard()).isEqualTo(Shard.partition(PartitionId.of("1")));
        assertThat(receipt.ref().orElseThrow().position()).isEqualTo("0");
        assertThat(connection
                        .resources()
                        .read(ResourceQuery.of(KafkaKinds.PARTITION)
                                .where(Map.of("topic", topic))
                                .project(Set.of(KafkaKinds.LOG_END_OFFSET)))
                        .orThrow()
                        .items()
                        .stream()
                        .mapToLong(r -> (Long)
                                r.attributes().raw(KafkaKinds.LOG_END_OFFSET).orElse(0L))
                        .sum())
                .isEqualTo(1);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
