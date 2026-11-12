package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Topic create, delete and partition increase, and deletion of an empty group. A preview changes nothing, an internal
 * name is refused before the broker is asked, and a group is never emptied of members in order to delete it.
 */
class KafkaTopicLifecycleIT {

    private static final String INTERNAL = "The cluster manages internal topics.";

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

    @Test
    void aCreatePreviewValidatesAndLeavesNoTopic() throws Exception {
        String name = Kafka.unique("create");
        ResourceId id = topic(name);
        Attributes attributes = Attributes.builder()
                .putRaw(KafkaKinds.PARTITIONS, 6L)
                .putRaw(KafkaKinds.REPLICATION_FACTOR, 3L)
                .putRaw(KafkaKinds.CONFIG_PREFIX + "cleanup.policy", "delete")
                .build();

        Preview preview = connection.operations().preview(id, new Operation.Create(attributes), Scope.whole());

        assertThat(preview.diff())
                .anySatisfy(change -> assertThat(change.after()).contains(6L))
                .anySatisfy(change -> assertThat(change.after()).contains(3L));
        assertThat(note(preview)).contains("Nothing has been created");
        assertAbsent(name);
        connection
                .operations()
                .execute(id, new Operation.Create(attributes), Scope.whole())
                .orThrow();
        assertThat(partitions(name)).isEqualTo(6);
    }

    @Test
    void anInternalNameIsRefusedBeforeTheBrokerIsAsked() throws Exception {
        String name = "__" + Kafka.unique("internal");
        ResourceId id = topic(name);

        assertThatThrownBy(() ->
                        connection.operations().preview(id, new Operation.Create(Attributes.empty()), Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.getMessage()).contains(INTERNAL);
                    assertThat(e.dispatch().name()).isEqualTo("NOT_SENT");
                });
        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(id, new Operation.Create(Attributes.empty()), Scope.whole())
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.getMessage()).contains(INTERNAL));
        assertAbsent(name);

        ResourceId offsets = topic("__consumer_offsets");
        assertThatThrownBy(() -> connection.operations().preview(offsets, Operation.delete(), Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.getMessage()).contains(INTERNAL));
        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(offsets, Operation.delete(), Scope.whole())
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.getMessage()).contains(INTERNAL));
        assertThat(names()).contains("__consumer_offsets");
    }

    @Test
    void aReplicationFactorAboveTheBrokersReachedIsRefused() {
        ResourceId id = topic(Kafka.unique("rf"));
        Attributes attributes =
                Attributes.builder().putRaw(KafkaKinds.REPLICATION_FACTOR, 9L).build();

        assertThatThrownBy(() -> connection.operations().preview(id, new Operation.Create(attributes), Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                    assertThat(e.getMessage()).contains("3");
                });
    }

    @Test
    void aSettingTheTopicKindDoesNotDeclareIsRefused() {
        ResourceId id = topic(Kafka.unique("setting"));
        Attributes attributes = Attributes.builder()
                .putRaw(KafkaKinds.CONFIG_PREFIX + "not.a.setting", "1")
                .build();

        assertThatThrownBy(() -> connection.operations().preview(id, new Operation.Create(attributes), Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                    assertThat(e.dispatch().name()).isEqualTo("NOT_SENT");
                });
    }

    @Test
    void aBrokerRejectionLeavesNoTopic() throws Exception {
        String name = Kafka.unique("rejected");
        ResourceId id = topic(name);
        Attributes attributes = Attributes.builder()
                .putRaw(KafkaKinds.PARTITIONS, 1L)
                .putRaw(KafkaKinds.REPLICATION_FACTOR, 1L)
                .putRaw(KafkaKinds.CONFIG_PREFIX + "cleanup.policy", "banana")
                .build();

        assertThatThrownBy(() -> connection.operations().preview(id, new Operation.Create(attributes), Scope.whole()))
                .isInstanceOf(BrokerException.class);
        assertAbsent(name);
        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(id, new Operation.Create(attributes), Scope.whole())
                        .orThrow())
                .isInstanceOf(BrokerException.class);
        assertAbsent(name);
    }

    @Test
    void deletingATopicNamesTheGroupsThatStillHaveOffsets() throws Exception {
        String name = topicWith(2);
        Kafka.produce(kafka, name, 4);
        String billing = Kafka.unique("billing");
        String search = Kafka.unique("search");
        commit(billing, name, 0, 0);
        commit(search, name, 1, 1);
        ResourceId id = topic(name);

        Preview preview = connection.operations().preview(id, Operation.delete(), Scope.whole());

        assertThat(groups(preview)).contains(billing, search);
        assertThat(preview.estimate().count()).isInstanceOf(Count.Estimate.class);
        assertThat(note(preview)).contains("does not erase").contains("overcount");
        connection
                .operations()
                .execute(id, Operation.delete(), Scope.whole(), Precondition.ifMatch(preview.etag()))
                .orThrow();
        assertAbsent(name);
    }

    @Test
    void deleteIfEmptyRefusesWhenTheEstimateIsAboveZero() throws Exception {
        String name = topicWith(1);
        Kafka.produce(kafka, name, 2);
        ResourceId id = topic(name);
        Operation delete = new Operation.Delete(true, false);

        assertThatThrownBy(() -> connection.operations().preview(id, delete, Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY);
                    assertThat(e.getMessage()).contains("estimate");
                });
        assertThat(names()).contains(name);
    }

    @Test
    void aStaleEtagDeletesNothing() throws Exception {
        String name = topicWith(2);
        ResourceId id = topic(name);
        Preview preview = connection.operations().preview(id, Operation.delete(), Scope.whole());
        connection
                .operations()
                .execute(
                        id,
                        new Operation.Update(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 4L)
                                .build()),
                        Scope.whole())
                .orThrow();

        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(id, Operation.delete(), Scope.whole(), Precondition.ifMatch(preview.etag()))
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED));
        assertThat(partitions(name)).isEqualTo(4);
    }

    @Test
    void increasingPartitionsValidatesBeforeItApplies() throws Exception {
        String name = topicWith(6);
        ResourceId id = topic(name);
        Operation increase = new Operation.Update(
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 12L).build());

        Preview preview = connection.operations().preview(id, increase, Scope.whole());

        assertThat(partitions(name)).isEqualTo(6);
        assertThat(note(preview)).contains("stay on their partitions").contains("new partition");
        connection
                .operations()
                .execute(id, increase, Scope.whole(), Precondition.ifMatch(preview.etag()))
                .orThrow();
        assertThat(partitions(name)).isEqualTo(12);
    }

    @Test
    void aShrinkNeverReachesTheBroker() throws Exception {
        String name = topicWith(6);
        ResourceId id = topic(name);
        Operation shrink = new Operation.Update(
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 2L).build());

        assertThatThrownBy(() -> connection.operations().preview(id, shrink, Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.getMessage()).contains("6").contains("2");
                    assertThat(e.dispatch().name()).isEqualTo("NOT_SENT");
                });
        assertThat(partitions(name)).isEqualTo(6);
    }

    @Test
    void aReplicaAssignmentNeverReachesTheBroker() throws Exception {
        String name = topicWith(6);
        ResourceId id = topic(name);
        Operation assigned = new Operation.Update(Attributes.builder()
                .putRaw(KafkaKinds.PARTITIONS, 12L)
                .putRaw(KafkaKinds.REPLICAS, List.of(1))
                .build());

        assertThatThrownBy(() -> connection.operations().preview(id, assigned, Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class,
                        e -> assertThat(e.dispatch().name()).isEqualTo("NOT_SENT"));
        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(id, assigned, Scope.whole())
                        .orThrow())
                .isInstanceOf(BrokerException.class);
        assertThat(partitions(name)).isEqualTo(6);
    }

    @Test
    void aBrokerRejectionAddsNoPartitions() throws Exception {
        String name = topicWith(1);
        ResourceId id = topic(name);
        admin.deleteTopics(List.of(name)).all().get();
        assertAbsent(name);
        Operation increase = new Operation.Update(
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 4L).build());

        assertThatThrownBy(() -> connection.operations().preview(id, increase, Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.getMessage()).isNotBlank());
        assertAbsent(name);
    }

    @Test
    void deletingAGroupNeverRemovesItsMembers() throws Exception {
        String name = topicWith(1);
        String busy = Kafka.unique("busy");
        String idle = Kafka.unique("idle");
        String other = Kafka.unique("other");
        commit(idle, name, 0);
        commit(other, name, 0);
        try (KafkaConsumer<byte[], byte[]> member = Kafka.member(kafka, busy, name, "classic")) {
            assertThat(member.assignment()).isNotEmpty();
            ResourceId id = group(busy);
            Operation delete = new Operation.Delete(false, true);

            assertThatThrownBy(() -> connection.operations().preview(id, delete, Scope.whole()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.reason()).isEqualTo(ErrorReasons.HAS_CONSUMERS);
                        assertThat(e.getMessage()).contains(busy);
                        assertThat(e.dispatch().name()).isEqualTo("NOT_SENT");
                    });
            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id, delete, Scope.whole())
                            .orThrow())
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.HAS_CONSUMERS));
            assertThat(admin.describeConsumerGroups(List.of(busy))
                            .all()
                            .get()
                            .get(busy)
                            .members())
                    .isNotEmpty();
        }

        ResourceId idleId = group(idle);
        Preview preview = connection.operations().preview(idleId, Operation.delete(), Scope.whole());
        assertThat(preview.diff()).anySatisfy(change -> assertThat(change.key()).isEqualTo(KafkaKinds.GROUP_STATE));
        connection
                .operations()
                .execute(idleId, Operation.delete(), Scope.whole(), Precondition.ifMatch(preview.etag()))
                .orThrow();
        assertThat(admin.listGroups().all().get().stream().map(g -> g.groupId()))
                .doesNotContain(idle)
                .contains(other);
        assertThat(admin.describeConsumerGroups(List.of(other))
                        .all()
                        .get()
                        .get(other)
                        .members())
                .isEmpty();
    }

    private static ResourceId topic(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static ResourceId group(String name) {
        return new ResourceId(KafkaKinds.GROUP, ResourcePath.of(name));
    }

    private static String topicWith(int partitions) throws Exception {
        String name = Kafka.unique("life");
        NewTopic topic = new NewTopic(name, partitions, (short) 3);
        // The fixture's broker default is min.insync.replicas=2. One replica cannot satisfy acks=all, and the
        // producer then waits out its delivery timeout.
        topic.configs(Map.of("min.insync.replicas", "1"));
        admin.createTopics(List.of(topic)).all().get();
        return name;
    }

    private static void commit(String group, String topic, long... offsets) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> at = new HashMap<>();
        for (int p = 0; p < offsets.length; p++) {
            at.put(new TopicPartition(topic, p), new OffsetAndMetadata(offsets[p]));
        }
        admin.alterConsumerGroupOffsets(group, at).all().get();
    }

    private static int partitions(String name) throws Exception {
        return admin.describeTopics(List.of(name))
                .allTopicNames()
                .get()
                .get(name)
                .partitions()
                .size();
    }

    private static Set<String> names() throws Exception {
        return admin.listTopics(new ListTopicsOptions().listInternal(true))
                .names()
                .get();
    }

    private static void assertAbsent(String name) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        do {
            assertThat(names()).doesNotContain(name);
            Thread.sleep(200);
        } while (System.nanoTime() < until);
    }

    private static String note(Preview preview) {
        return preview.diff().stream()
                .filter(change -> KafkaKinds.NOTE.equals(change.key()))
                .map(change -> String.valueOf(change.after().orElse("")))
                .findFirst()
                .orElse("");
    }

    private static List<String> groups(Preview preview) {
        return preview.diff().stream()
                .map(change -> change.key())
                .filter(key -> key.startsWith(KafkaKinds.OFFSETS_KEPT))
                .map(key -> key.substring(KafkaKinds.OFFSETS_KEPT.length()))
                .toList();
    }
}
