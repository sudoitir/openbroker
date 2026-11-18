package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Tiered offsets and the election and reassignment refusals on Kafka 2.8 (ZooKeeper) and 3.9 (KRaft). 2.8 does not
 * serve the tiered offset specs. 3.9 does, and a topic that is not tiered still reports them as unknown.
 */
class KafkaReplicationVersionsIT {

    private static final String IMAGE_28 = System.getProperty("kafka.28.image", "confluentinc/cp-kafka:6.2.15");
    private static final String IMAGE_39 = "apache/kafka:3.9.2";
    private static final String THROTTLE = "replication.throttle.bytes_per_second";

    @Test
    void kafka28DeclaresTieredOffsetsUnavailableAndRefusesAMove() {
        try (BrokerCluster cluster = zooKeeper28()) {
            try (BrokerDrivers drivers = BrokerDrivers.load();
                    ClusterConnection connection = connect(drivers, cluster.bootstrap())) {
                assertThat(connection
                                .access()
                                .probe()
                                .of(KafkaAccess.TIERED_OFFSETS)
                                .status())
                        .isEqualTo(Assessment.Status.UNAVAILABLE);
                assertThat(connection
                                .access()
                                .probe()
                                .of(KafkaAccess.TIERED_OFFSETS)
                                .reason())
                        .isNotBlank();
                refusals(connection);
            }
        }
    }

    @Test
    void kafka39ReadsTieredOffsetsAndRefusesAMove() {
        try (KafkaContainer container = new KafkaContainer(IMAGE_39)) {
            container.start();
            try (BrokerDrivers drivers = BrokerDrivers.load();
                    ClusterConnection connection = connect(drivers, container.getBootstrapServers())) {
                String name = Kafka.unique("tier39");
                create(connection, name);
                try {
                    Resource partition = Kafka.eventually(
                            () -> partition(connection, name),
                            resource -> resource != null
                                    && resource.attributes()
                                            .raw(KafkaKinds.LOCAL_LOG_START_OFFSET)
                                            .isPresent());
                    assertThat(partition.attributes().raw(KafkaKinds.LOCAL_LOG_START_OFFSET))
                            .isEqualTo(partition.attributes().raw(KafkaKinds.LOG_START_OFFSET));
                    assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET))
                            .isEmpty();
                    assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET_REASON))
                            .contains("not tiered");
                    assertThat(connection
                                    .access()
                                    .probe()
                                    .of(KafkaAccess.TIERED_OFFSETS)
                                    .status())
                            .isEqualTo(Assessment.Status.AVAILABLE);
                } finally {
                    delete(connection, name);
                }
                refusals(connection);
            }
        }
    }

    /** Unclean election, a stale etag, a missing throttle and a factor below min.insync.replicas change nothing. */
    private static void refusals(ClusterConnection connection) {
        String name = Kafka.unique("refuse");
        create(connection, name);
        try {
            ResourceId id = partitionId(name);
            Resource before = Kafka.eventually(
                    () -> partition(connection, name),
                    resource -> resource != null
                            && resource.attributes().raw(KafkaKinds.REPLICAS).isPresent()
                            && resource.attributes().raw(KafkaKinds.LEADER).isPresent());
            List<Integer> replicas = replicas(before);

            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id, new Operation.ElectLeader(Election.UNCLEAN), Scope.whole())
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

            Operation missing = reassign(replicas, Attributes.empty());
            assertThatThrownBy(() -> connection.operations().preview(id, missing, Scope.whole()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.getMessage()).contains(THROTTLE);
                    });
            Operation tooSmall = reassign(
                    List.of(replicas.getFirst()),
                    Attributes.builder().putRaw(THROTTLE, 1024L).build());
            assertThatThrownBy(() -> connection.operations().preview(id, tooSmall, Scope.whole()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.getMessage()).contains("1").contains("2");
                    });
            Resource after = partition(connection, name);
            assertThat(after).isNotNull();
            assertThat(replicas(after)).containsExactlyElementsOf(replicas);
            assertThat(after.attributes().raw(KafkaKinds.LEADER))
                    .isEqualTo(before.attributes().raw(KafkaKinds.LEADER));
        } finally {
            delete(connection, name);
        }
    }

    private static void create(ClusterConnection connection, String name) {
        connection
                .operations()
                .execute(
                        topicId(name),
                        new Operation.Create(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 1L)
                                .putRaw(KafkaKinds.REPLICATION_FACTOR, 1L)
                                .putRaw(KafkaKinds.CONFIG_PREFIX + "min.insync.replicas", "2")
                                .build()),
                        Scope.whole())
                .orThrow();
    }

    private static void delete(ClusterConnection connection, String name) {
        try {
            connection
                    .operations()
                    .execute(topicId(name), Operation.delete(), Scope.whole())
                    .orThrow();
        } catch (RuntimeException ignored) {
            // Already gone.
        }
    }

    private static ResourceId topicId(String name) {
        return new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
    }

    private static ResourceId partitionId(String topic) {
        return new ResourceId(KafkaKinds.PARTITION, ResourcePath.of(topic, "0"));
    }

    private static Resource partition(ClusterConnection connection, String topic) {
        return connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.PARTITION).where(Map.of("topic", topic)))
                .orThrow()
                .items()
                .stream()
                .findFirst()
                .orElse(null);
    }

    private static List<Integer> replicas(Resource resource) {
        List<Integer> ids = new ArrayList<>();
        for (Object id :
                (List<?>) resource.attributes().raw(KafkaKinds.REPLICAS).orElseThrow()) {
            ids.add(((Number) id).intValue());
        }
        return ids;
    }

    private static Operation reassign(List<Integer> replicas, Attributes options) {
        List<NodeId> nodes =
                replicas.stream().map(id -> NodeId.of(Integer.toString(id))).toList();
        return new Operation.Reassign(Map.of(PartitionId.of("0"), nodes), options);
    }

    private static ClusterConnection connect(BrokerDrivers drivers, String bootstrap) {
        ClusterSpec spec = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, bootstrap)), Map.of());
        return drivers.connect(
                new ConnectRequest(KafkaDriver.SYSTEM, spec, CredentialSource.none(), ConnectOptions.defaults()));
    }

    private static BrokerCluster zooKeeper28() {
        Network network = Network.newNetwork();
        GenericContainer<?> zookeeper = new GenericContainer<>(IMAGE_28.replace("cp-kafka", "cp-zookeeper"))
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .waitingFor(Wait.forLogMessage(".*binding to port.*", 1));
        zookeeper.start();
        ZooKeeperKafkaBroker broker = new ZooKeeperKafkaBroker(IMAGE_28, "kafka-1")
                .withSettings(Map.of(
                        "KAFKA_BROKER_ID",
                        "1",
                        "KAFKA_ZOOKEEPER_CONNECT",
                        "zookeeper:2181",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
                        "1"))
                .withNetwork(network)
                .withNetworkAliases("kafka-1");
        try {
            broker.start();
        } catch (RuntimeException e) {
            broker.stop();
            zookeeper.stop();
            network.close();
            throw e;
        }
        return new BrokerCluster(broker.getBootstrapServers(), () -> {
            broker.stop();
            zookeeper.stop();
            network.close();
        });
    }

    private record BrokerCluster(String bootstrap, Runnable stop) implements AutoCloseable {
        @Override
        public void close() {
            stop.run();
        }
    }
}
