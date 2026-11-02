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
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.admin.KafkaAccess;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A Kafka 2.8 cluster (Confluent Platform 6.2) connects: it reports no {@code metadata.version}, so its release is
 * read from the API keys it serves, and the transaction listing it lacks is declared with its reason while every
 * other capability is read as usual (ADR-0032). A cluster below 2.8 is still refused; see
 * {@link KafkaBefore28ZooKeeperIT}.
 */
class Kafka28ZooKeeperIT {

    private static final String IMAGE = System.getProperty("kafka.28.image", "confluentinc/cp-kafka:6.2.15");

    private static final KindId HANGING_TRANSACTION = KindId.of("kafka.hanging-transaction");

    private static final KindId TRANSACTIONAL_ID = KindId.of("kafka.transactional-id");

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

    private static ClusterSpec spec() {
        return new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, broker.getBootstrapServers())),
                Map.of());
    }

    private static ClusterConnection connect(BrokerDrivers drivers) {
        return drivers.connect(
                new ConnectRequest(KafkaDriver.SYSTEM, spec(), CredentialSource.none(), ConnectOptions.defaults()));
    }

    @Test
    void verifyAcceptsKafka28AndNamesTheTransactionListingItLacks() {
        try (BrokerDrivers drivers = BrokerDrivers.load();
                ClusterConnection connection = connect(drivers)) {
            assertThat(connection.nodes().describe(Scope.whole()).value().stream()
                            .map(node -> node.attributes()
                                    .raw("kafka.cluster_version")
                                    .orElseThrow())
                            .map(String::valueOf)
                            .toList())
                    .as("every broker reports the release the API keys named")
                    .isNotEmpty()
                    .containsOnly("at least 2.8");
            assertThat(connection
                            .access()
                            .probe()
                            .of(KafkaAccess.TRANSACTION_LISTING)
                            .status())
                    .isEqualTo(Assessment.Status.UNAVAILABLE);
            assertThat(connection
                            .access()
                            .probe()
                            .of(KafkaAccess.TRANSACTION_LISTING)
                            .reason())
                    .contains("Kafka 3.0 or later");
        }
    }

    @Test
    void theTransactionKindsAreNotListedAndReadingThemNamesWhy() {
        try (BrokerDrivers drivers = BrokerDrivers.load();
                ClusterConnection connection = connect(drivers)) {
            assertThat(connection.resources().kinds().stream().map(k -> k.id().value()))
                    .as("listing transactional ids and hanging transactions needs Kafka 3.0")
                    .contains("kafka.topic")
                    .doesNotContain(TRANSACTIONAL_ID.value(), HANGING_TRANSACTION.value());

            for (KindId kind : List.of(TRANSACTIONAL_ID, HANGING_TRANSACTION)) {
                assertThatThrownBy(() -> connection
                                .resources()
                                .read(ResourceQuery.of(kind))
                                .orThrow())
                        .as(kind.value())
                        .isInstanceOfSatisfying(BrokerException.class, e -> {
                            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                            assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
                            assertThat(e.getMessage()).contains("Kafka 3.0 or later");
                        });
            }
        }
    }
}
