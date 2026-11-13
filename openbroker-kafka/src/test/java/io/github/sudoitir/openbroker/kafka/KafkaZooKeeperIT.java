package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.admin.KafkaAccess;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.kafka.admin.KafkaNodes;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * What the driver reports of a cluster that keeps its metadata in ZooKeeper (the {@code zookeeper} profile). A dynamic
 * broker setting is changed through the same Admin call as any other cluster; a setting only a restart changes stays
 * read-only. Topic create, delete and a partition increase use the same operations as any other cluster.
 */
class KafkaZooKeeperIT {

    private static final String SETTING = KafkaKinds.CONFIG_PREFIX + "message.max.bytes";

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        assumeTrue(kafka.zookeeperMode(), "Needs a ZooKeeper-mode cluster: run with the zookeeper profile");
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
    }

    @AfterAll
    static void stop() {
        if (connection != null) {
            connection.close();
            drivers.close();
        }
    }

    @Test
    void verifyReachesEveryBrokerAndDescribesTheClusterAsZooKeeper() {
        var nodes = drivers.verify(new ConnectRequest(
                        KafkaDriver.SYSTEM, kafka.spec(), CredentialSource.none(), ConnectOptions.defaults()))
                .nodes();

        assertThat(nodes.failures()).isEmpty();
        assertThat(nodes.value()).hasSize(3);
        assertThat(nodes.outcomes().values()).containsOnly(NodeOutcome.APPLIED);
        assertThat(nodes.value())
                .allSatisfy(n ->
                        assertThat(n.attributes().raw(KafkaNodes.CLUSTER_MODE)).contains("zookeeper"));
        assertThat(nodes.value()).extracting(NodeDescription::nativeId).containsExactlyInAnyOrder("1", "2", "3");
    }

    @Test
    void whatTheClusterLacksIsDeclaredWithItsReason() {
        Assessment quorum = connection.access().check(KafkaAccess.QUORUM, Optional.empty());
        Assessment protocol = connection.access().check(KafkaAccess.CONSUMER_PROTOCOL_GROUPS, Optional.empty());

        assertThat(quorum.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(protocol.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(protocol.reason()).contains("Kafka 4.0 or later");
    }

    @Test
    void aDynamicBrokerSettingChangesAndIsNotReadOnly() {
        assertThat(readOnly("1")).contains("log.dirs").doesNotContain("message.max.bytes");
        ResourceId all = config("*");
        try {
            connection
                    .operations()
                    .execute(all, update(SETTING, "2097152"), Scope.whole())
                    .orThrow();

            Resource changed = Kafka.eventually(
                    () -> listed(all),
                    resource -> resource.attributes().raw(SETTING).equals(Optional.of("2097152")));
            assertThat(changed.attributes().raw(SETTING)).contains("2097152");
            assertThat(configSources(changed)).contains("message.max.bytes=cluster");
        } finally {
            connection
                    .operations()
                    .execute(all, update(SETTING, ""), Scope.whole())
                    .orThrow();
        }
        Resource cleared = Kafka.eventually(() -> listed(all), KafkaZooKeeperIT::dynamicOverrideCleared);
        assertThat(cleared.attributes().raw(SETTING)).isNotEqualTo(Optional.of("2097152"));
        assertThat(configSources(cleared)).doesNotContain("message.max.bytes=cluster");
    }

    @Test
    void topicCreateDeleteAndPartitionIncrease() {
        try (Admin admin = Kafka.admin(kafka)) {
            String name = Kafka.unique("zk30");
            ResourceId id = new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
            Attributes created = Attributes.builder()
                    .putRaw(KafkaKinds.PARTITIONS, 1L)
                    .putRaw(KafkaKinds.REPLICATION_FACTOR, 1L)
                    .build();

            connection
                    .operations()
                    .execute(id, new Operation.Create(created), Scope.whole())
                    .orThrow();
            assertThat(partitions(admin, name)).isEqualTo(1);

            connection
                    .operations()
                    .execute(
                            id,
                            new Operation.Update(Attributes.builder()
                                    .putRaw(KafkaKinds.PARTITIONS, 2L)
                                    .build()),
                            Scope.whole())
                    .orThrow();
            assertThat(Kafka.eventually(() -> partitions(admin, name), count -> count == 2))
                    .isEqualTo(2);

            connection
                    .operations()
                    .execute(id, Operation.delete(), Scope.whole())
                    .orThrow();
            assertThat(Kafka.eventually(() -> absent(admin, name), Boolean.TRUE::equals))
                    .isTrue();
        }
    }

    @Test
    void aSettingOnlyARestartChangesIsRefusedBeforeAnythingIsSent() {
        for (String broker : List.of("*", "1")) {
            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(
                                    config(broker),
                                    update(KafkaKinds.CONFIG_PREFIX + "log.dirs", "/tmp/x"),
                                    Scope.whole())
                            .orThrow())
                    .isInstanceOfSatisfying(BrokerException.class, exception -> {
                        assertThat(exception.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(exception.reason()).isEqualTo(ErrorReasons.IMMUTABLE_FIELD);
                        assertThat(exception.getMessage()).contains("server.properties");
                    });
        }
    }

    private static ResourceId config(String name) {
        return new ResourceId(KafkaKinds.BROKER_CONFIG, ResourcePath.of(name));
    }

    private static Operation update(String key, String value) {
        return new Operation.Update(Attributes.builder().putRaw(key, value).build());
    }

    private static Resource listed(ResourceId id) {
        return connection.resources().read(ResourceQuery.of(KafkaKinds.BROKER_CONFIG)).orThrow().items().stream()
                .filter(resource -> resource.id().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private static boolean dynamicOverrideCleared(Resource resource) {
        return !resource.attributes().raw(SETTING).equals(Optional.of("2097152"))
                && !configSources(resource).contains("message.max.bytes=cluster");
    }

    private static String configSources(Resource resource) {
        return resource.attributes()
                .raw(KafkaKinds.CONFIG_SOURCES)
                .map(String::valueOf)
                .orElse("");
    }

    private static int partitions(Admin admin, String name) {
        try {
            return admin.describeTopics(List.of(name))
                    .allTopicNames()
                    .get()
                    .get(name)
                    .partitions()
                    .size();
        } catch (Exception _) {
            return -1;
        }
    }

    private static boolean absent(Admin admin, String name) {
        try {
            return !admin.listTopics().names().get().contains(name);
        } catch (Exception _) {
            return false;
        }
    }

    private static String readOnly(String broker) {
        return listed(config(broker))
                .attributes()
                .raw(KafkaKinds.CONFIG_READ_ONLY)
                .map(String::valueOf)
                .orElse("");
    }
}
