package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.admin.KafkaAccess;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Election;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Partition placement on the default image: tiered offsets, preferred election and a throttled replica move. */
class KafkaReplicationIT {

    private static final String THROTTLE = "replication.throttle.bytes_per_second";

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

    @Test
    void aTopicThatIsNotTieredReportsLocalStartAndAnUnknownTieredOffset() {
        String name = Kafka.unique("tier");
        create(name, 3, Map.of());
        try {
            Resource partition = awaitPartition(
                    name,
                    resource -> resource.attributes()
                            .raw(KafkaKinds.LOCAL_LOG_START_OFFSET)
                            .isPresent());

            assertThat(partition.attributes().raw(KafkaKinds.LOCAL_LOG_START_OFFSET))
                    .isEqualTo(partition.attributes().raw(KafkaKinds.LOG_START_OFFSET));
            assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET)).isEmpty();
            assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET_REASON))
                    .contains("not tiered");
            assertThat(connection
                            .access()
                            .probe()
                            .of(KafkaAccess.TIERED_OFFSETS)
                            .status())
                    .isEqualTo(Assessment.Status.AVAILABLE);
        } finally {
            delete(name);
        }
    }

    @Test
    void anAlreadyPreferredLeaderIsLeftAsItIs() {
        KafkaReplicationChecks.anAlreadyPreferredLeaderIsLeftAsItIs(connection);
    }

    @Test
    void uncleanElectionAndAStaleEtagSendNothing() {
        String name = Kafka.unique("elect");
        create(name, 3, Map.of());
        try {
            ResourceId id = partitionId(name);
            Resource before = awaitPartition(
                    name,
                    resource -> resource.attributes().raw(KafkaKinds.LEADER).isPresent());
            Operation unclean = new Operation.ElectLeader(Election.UNCLEAN);
            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id, unclean, Scope.whole())
                            .orThrow())
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));

            Operation preferred = new Operation.ElectLeader(Election.PREFERRED);
            connection.operations().preview(id, preferred, Scope.whole());
            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id, preferred, Scope.whole(), Precondition.ifMatch("stale"))
                            .orThrow())
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
            assertThat(awaitPartition(name, resource -> true).attributes().raw(KafkaKinds.LEADER))
                    .isEqualTo(before.attributes().raw(KafkaKinds.LEADER));
        } finally {
            delete(name);
        }
    }

    @Test
    void aMissingThrottleAndAFactorBelowMinInSyncDoNotMoveReplicas() {
        String name = Kafka.unique("refuse");
        create(name, 3, Map.of("min.insync.replicas", "2"));
        try {
            ResourceId id = partitionId(name);
            List<Integer> before = replicas(awaitPartition(
                    name,
                    resource -> resource.attributes().raw(KafkaKinds.REPLICAS).isPresent()));
            Operation missing = reassign(before, Attributes.empty());
            assertThatThrownBy(() -> connection.operations().preview(id, missing, Scope.whole()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.getMessage()).contains(THROTTLE);
                    });

            Operation tooSmall = reassign(
                    List.of(before.getFirst()),
                    Attributes.builder().putRaw(THROTTLE, 1024L).build());
            assertThatThrownBy(() -> connection.operations().preview(id, tooSmall, Scope.whole()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.getMessage()).contains("1").contains("2");
                    });
            assertThat(replicas(awaitPartition(name, resource -> true))).containsExactlyElementsOf(before);
        } finally {
            delete(name);
        }
    }

    @Test
    void aStaleReassignmentEtagDoesNotMoveReplicas() {
        String name = Kafka.unique("etag");
        create(name, 2, Map.of("min.insync.replicas", "1"));
        try {
            ResourceId id = partitionId(name);
            List<Integer> before = replicas(awaitPartition(
                    name,
                    resource -> resource.attributes().raw(KafkaKinds.REPLICAS).isPresent()));
            Operation move = reassign(withOneMore(before), throttle(1024));
            Preview preview = connection.operations().preview(id, move, Scope.whole());

            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id, move, Scope.whole(), Precondition.ifMatch("stale"))
                            .orThrow())
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
            assertThat(preview.etag()).isNotEqualTo("stale");
            assertThat(replicas(awaitPartition(name, resource -> true))).containsExactlyElementsOf(before);
        } finally {
            delete(name);
        }
    }

    @Test
    void addingAReplicaIsListedAndCancelRestoresTheReplicas() {
        KafkaReplicationChecks.addingAReplicaIsListedAndCancelRestores(connection, kafka);
    }

    private static void create(String name, long replicas, Map<String, String> configs) {
        Attributes.Builder attributes =
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 1L).putRaw(KafkaKinds.REPLICATION_FACTOR, replicas);
        configs.forEach((key, value) -> attributes.putRaw(KafkaKinds.CONFIG_PREFIX + key, value));
        connection
                .operations()
                .execute(topicId(name), new Operation.Create(attributes.build()), Scope.whole())
                .orThrow();
    }

    private static void delete(String name) {
        try {
            connection
                    .operations()
                    .execute(topicId(name), Operation.delete(), Scope.whole())
                    .orThrow();
        } catch (RuntimeException ignored) {
            // The topic may already be gone. A leftover throttle is cleared by the caller.
        }
    }

    private static ResourceId topicId(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static ResourceId partitionId(String topic) {
        return new ResourceId(KafkaKinds.PARTITION, ResourcePath.of(topic, "0"));
    }

    private static Resource awaitPartition(String topic, Predicate<Resource> settled) {
        Resource resource = Kafka.eventually(
                () -> partition(topic).orElse(null), candidate -> candidate != null && settled.test(candidate));
        assertThat(resource).isNotNull();
        return resource;
    }

    private static Optional<Resource> partition(String topic) {
        return connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.PARTITION).where(Map.of("topic", topic)))
                .orThrow()
                .items()
                .stream()
                .findFirst();
    }

    private static List<Integer> replicas(Resource resource) {
        List<Integer> ids = new ArrayList<>();
        for (Object id :
                (List<?>) resource.attributes().raw(KafkaKinds.REPLICAS).orElseThrow()) {
            ids.add(((Number) id).intValue());
        }
        return ids;
    }

    private static List<Integer> withOneMore(List<Integer> current) {
        Set<Integer> have = new HashSet<>(current);
        int extra = -1;
        for (int id = 1; id <= 3; id++) {
            if (!have.contains(id)) {
                extra = id;
            }
        }
        assertThat(extra).isPositive();
        List<Integer> next = new ArrayList<>(current);
        next.add(extra);
        return next;
    }

    private static Operation reassign(List<Integer> replicas, Attributes options) {
        List<NodeId> nodes =
                replicas.stream().map(id -> NodeId.of(Integer.toString(id))).toList();
        return new Operation.Reassign(Map.of(PartitionId.of("0"), nodes), options);
    }

    private static Attributes throttle(long bytesPerSecond) {
        return Attributes.builder().putRaw(THROTTLE, bytesPerSecond).build();
    }
}
