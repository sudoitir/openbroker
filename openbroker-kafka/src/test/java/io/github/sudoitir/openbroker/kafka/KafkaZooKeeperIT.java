package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.admin.KafkaAccess;
import io.github.sudoitir.openbroker.kafka.admin.KafkaNodes;
import io.github.sudoitir.openbroker.model.NodeDescription;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** What the driver reports of a cluster that keeps its metadata in ZooKeeper (the {@code zookeeper} profile). */
class KafkaZooKeeperIT {

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
}
