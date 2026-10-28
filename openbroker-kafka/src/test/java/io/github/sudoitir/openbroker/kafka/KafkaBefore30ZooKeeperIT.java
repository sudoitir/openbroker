package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A cluster before Kafka 3.0 (Kafka 2.8 in ZooKeeper mode) is refused at verify, and what it was
 * refused for is named. This also proves that ListTransactions, which Kafka 3.0 introduced, is the
 * marker that tells a cluster too old to report its metadata version from one that is supported.
 */
class KafkaBefore30ZooKeeperIT {

    private static final String IMAGE = System.getProperty("kafka.before30.image", "confluentinc/cp-kafka:6.2.15");

    private static Network network;
    private static GenericContainer<?> zookeeper;
    private static ZooKeeperKafkaBroker broker;

    @BeforeAll
    static void start() {
        network = Network.newNetwork();
        zookeeper = new GenericContainer<>(IMAGE.replace("cp-kafka", "cp-zookeeper"))
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .waitingFor(Wait.forLogMessage(".*binding to port.*", 1));
        zookeeper.start();
        broker = new ZooKeeperKafkaBroker(IMAGE, "kafka-1")
                .withSettings(Map.of(
                        "KAFKA_BROKER_ID", "1",
                        "KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"))
                .withNetwork(network)
                .withNetworkAliases("kafka-1");
        broker.start();
    }

    @AfterAll
    static void stop() {
        broker.stop();
        zookeeper.stop();
        network.close();
    }

    @Test
    void verifyRefusesAClusterBeforeKafka30() {
        ClusterSpec spec = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, broker.getBootstrapServers())),
                Map.of());

        try (BrokerDrivers drivers = BrokerDrivers.load()) {
            assertThatThrownBy(() -> drivers.verify(new ConnectRequest(
                            KafkaDriver.SYSTEM, spec, CredentialSource.none(), ConnectOptions.defaults())))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
                        assertThat(e.getMessage()).contains("before 3.0").contains("[3.0.0,)");
                    });
        }
    }
}
