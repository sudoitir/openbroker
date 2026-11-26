package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Election;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

/**
 * The replica-move and preferred-election checks the version matrix shares. A successful move needs three brokers: the
 * topic starts with two replicas and the third is added, then the move is cancelled.
 */
final class KafkaReplicationChecks {

    private static final String THROTTLE = "replication.throttle.bytes_per_second";
    private static final String FOLLOWER_RATE = "follower.replication.throttled.rate";
    private static final String LEADER_RATE = "leader.replication.throttled.rate";
    private static final String LEADER_REPLICAS = "leader.replication.throttled.replicas";
    private static final String FOLLOWER_REPLICAS = "follower.replication.throttled.replicas";

    private KafkaReplicationChecks() {}

    /** An already-preferred leader is left as it is: the call reports {@code alreadyInState} and sends nothing. */
    static void anAlreadyPreferredLeaderIsLeftAsItIs(ClusterConnection connection) {
        String name = Kafka.unique("prefer");
        create(connection, name, 3, Map.of());
        try {
            ResourceId id = partitionId(name);
            Operation elect = new Operation.ElectLeader(Election.PREFERRED);
            OperationResult result = Kafka.eventually(
                    () -> connection
                            .operations()
                            .execute(id, elect, Scope.whole())
                            .orThrow(),
                    OperationResult::alreadyInState);

            assertThat(result.alreadyInState()).isTrue();
        } finally {
            delete(connection, name);
        }
    }

    /**
     * Adding a replica is listed, every replica reports its size and lag, and cancelling restores the original set. The
     * throttle this call introduces is cleared afterwards, including when the move fails.
     */
    static void addingAReplicaIsListedAndCancelRestores(ClusterConnection connection, KafkaTestCluster kafka) {
        String name = Kafka.unique("move");
        create(connection, name, 2, Map.of("min.insync.replicas", "1"));
        int kept = -1;
        try (Admin admin = Kafka.admin(kafka)) {
            try {
                Kafka.produce(kafka, name, 200);
                ResourceId id = partitionId(name);
                Resource before = awaitPartition(connection, name, KafkaReplicationChecks::replicasReported);
                assertThat(before.placements()).hasSizeGreaterThanOrEqualTo(2);
                assertThat(before.placements()).allSatisfy(placement -> {
                    assertThat(placement.attributes().raw(KafkaKinds.REPLICA_SIZE_BYTES))
                            .isPresent();
                    assertThat(placement.attributes().raw(KafkaKinds.OFFSET_LAG))
                            .isPresent();
                });
                List<Integer> original = replicas(before);
                kept = original.getFirst();
                Operation move = reassign(withOneMore(original), throttle(1024));
                Preview preview = connection.operations().preview(id, move, Scope.whole());
                assertThat(preview.estimate().count()).isInstanceOfAny(Count.Estimate.class, Count.Unavailable.class);

                OperationResult result = apply(connection, id, move);
                Resource reported = result.resource().orElseThrow();
                // A rate this call introduces is marked added. Reading a rate that was already set is
                // covered by ReplicaMovesTest: 3.9 and 4.0 do not always echo it in time.
                assertThat(added(reported)).anyMatch(key -> key.contains(FOLLOWER_RATE));

                Resource moving = Kafka.eventually(
                        () -> partition(connection, name).orElse(null),
                        resource -> resource != null
                                && resource.attributes()
                                        .raw(KafkaKinds.REASSIGNMENT)
                                        .isPresent());
                assertThat(moving.attributes().raw(KafkaKinds.REASSIGNMENT)).isPresent();

                Operation cancel = new Operation.Reassign(Map.of(PartitionId.of("0"), List.of()));
                apply(connection, id, cancel);
                List<Integer> restored = replicas(Kafka.eventually(
                        () -> partition(connection, name).orElse(null),
                        resource -> resource != null
                                && resource.attributes()
                                        .raw(KafkaKinds.REASSIGNMENT)
                                        .isEmpty()
                                && replicas(resource).equals(original)));
                assertThat(restored).containsExactlyElementsOf(original);
            } finally {
                if (kept > 0) {
                    clearThrottle(admin, kept);
                }
                clearTopicThrottles(admin, name);
            }
        } finally {
            delete(connection, name);
        }
    }

    /**
     * Preview and apply, and preview again when the in-sync set moves between the two. That is the same refusal the
     * caller gets, and on a three-broker cluster it happens while followers catch up.
     */
    private static OperationResult apply(ClusterConnection connection, ResourceId id, Operation operation) {
        BrokerException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            Preview preview = connection.operations().preview(id, operation, Scope.whole());
            try {
                return connection
                        .operations()
                        .execute(id, operation, Scope.whole(), Precondition.ifMatch(preview.etag()))
                        .orThrow();
            } catch (BrokerException e) {
                if (e.status() != Status.FAILED_PRECONDITION) {
                    throw e;
                }
                last = e;
            }
        }
        throw last;
    }

    private static boolean replicasReported(Resource resource) {
        if (resource.placements().size() < 2) {
            return false;
        }
        for (Placement placement : resource.placements()) {
            if (placement.attributes().raw(KafkaKinds.REPLICA_SIZE_BYTES).isEmpty()
                    || placement.attributes().raw(KafkaKinds.OFFSET_LAG).isEmpty()) {
                return false;
            }
        }
        return resource.attributes().raw(KafkaKinds.REPLICAS).isPresent();
    }

    private static void create(ClusterConnection connection, String name, long replicas, Map<String, String> configs) {
        Attributes.Builder attributes =
                Attributes.builder().putRaw(KafkaKinds.PARTITIONS, 1L).putRaw(KafkaKinds.REPLICATION_FACTOR, replicas);
        configs.forEach((key, value) -> attributes.putRaw(KafkaKinds.CONFIG_PREFIX + key, value));
        connection
                .operations()
                .execute(topicId(name), new Operation.Create(attributes.build()), Scope.whole())
                .orThrow();
    }

    private static void delete(ClusterConnection connection, String name) {
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

    private static Resource awaitPartition(ClusterConnection connection, String topic, Predicate<Resource> settled) {
        Resource resource = Kafka.eventually(
                () -> partition(connection, topic).orElse(null),
                candidate -> candidate != null && settled.test(candidate));
        assertThat(resource).isNotNull();
        return resource;
    }

    private static Optional<Resource> partition(ClusterConnection connection, String topic) {
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

    @SuppressWarnings("unchecked")
    private static List<String> added(Resource resource) {
        return new ArrayList<>((Collection<String>)
                resource.attributes().raw(KafkaKinds.THROTTLE_ADDED).orElse(List.of()));
    }

    private static void clearThrottle(Admin admin, int broker) {
        deleteOverride(admin, new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(broker)), LEADER_RATE);
        deleteOverride(admin, new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(broker)), FOLLOWER_RATE);
    }

    private static void clearTopicThrottles(Admin admin, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        deleteOverride(admin, resource, LEADER_REPLICAS);
        deleteOverride(admin, resource, FOLLOWER_REPLICAS);
    }

    private static void deleteOverride(Admin admin, ConfigResource resource, String key) {
        try {
            admin.incrementalAlterConfigs(Map.of(
                            resource,
                            List.of(new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE))))
                    .all()
                    .get();
        } catch (Exception ignored) {
            // An override that was never set has nothing to delete.
        }
    }
}
