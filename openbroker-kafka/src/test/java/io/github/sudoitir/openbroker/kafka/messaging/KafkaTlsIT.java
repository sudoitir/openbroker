package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * Mutual TLS: a broker whose client listener requires a certificate signed by a throwaway authority,
 * reached with no credentials at all. The material is asked for at every new connection, and the
 * broker's host name is checked against its certificate.
 */
class KafkaTlsIT {

    private static TestPki pki;
    private static GenericContainer<?> broker;
    private static int port;
    private static BrokerDrivers drivers;

    @BeforeAll
    static void start() throws IOException {
        pki = new TestPki();
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        String secrets = "/etc/kafka/secrets/";
        broker = new GenericContainer<>(KafkaTestCluster.kraftImage())
                .withEnv(Map.ofEntries(
                        Map.entry("KAFKA_NODE_ID", "1"),
                        Map.entry("KAFKA_PROCESS_ROLES", "broker,controller"),
                        Map.entry(
                                "KAFKA_LISTENERS",
                                "SSL://0.0.0.0:" + port + ",BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093"),
                        Map.entry("KAFKA_ADVERTISED_LISTENERS", "SSL://localhost:" + port + ",BROKER://localhost:9094"),
                        Map.entry(
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                "SSL:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                        Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
                        Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                        Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
                        // The image reads the stores from its secrets directory and their passwords from a file.
                        Map.entry("KAFKA_SSL_KEYSTORE_FILENAME", "broker.p12"),
                        Map.entry("KAFKA_SSL_KEYSTORE_CREDENTIALS", "credentials"),
                        Map.entry("KAFKA_SSL_KEY_CREDENTIALS", "credentials"),
                        Map.entry("KAFKA_SSL_TRUSTSTORE_FILENAME", "broker-trust.p12"),
                        Map.entry("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "credentials"),
                        Map.entry("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"),
                        Map.entry("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12"),
                        Map.entry("KAFKA_SSL_CLIENT_AUTH", "required"),
                        Map.entry("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", ""),
                        Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                        Map.entry("KAFKA_DEFAULT_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS", "250"),
                        Map.entry("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS", "500"),
                        Map.entry("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m")))
                .withCopyFileToContainer(
                        MountableFile.forHostPath(pki.file("broker.p12"), 0644), secrets + "broker.p12")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(pki.file("broker-trust.p12"), 0644), secrets + "broker-trust.p12")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(pki.file("credentials"), 0644), secrets + "credentials")
                .withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
        broker.start();
        drivers = BrokerDrivers.load();
    }

    @AfterAll
    static void stop() {
        drivers.close();
        broker.stop();
    }

    private static ClusterSpec spec() {
        return new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "localhost:" + port)),
                Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SSL));
    }

    private static ClusterConnection connect(TlsProvider tls) {
        return drivers.connect(new ConnectRequest(
                KafkaDriver.SYSTEM,
                spec(),
                CredentialSource.none(),
                ConnectOptions.defaults().withTls(tls)));
    }

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static String topic() {
        String name = "t.tls." + System.nanoTime();
        try (Admin admin = Admin.create(Map.of(
                "bootstrap.servers",
                "localhost:" + port,
                "security.protocol",
                "SSL",
                "ssl.keystore.type",
                "PKCS12",
                "ssl.keystore.location",
                pki.file("client.p12").toString(),
                "ssl.keystore.password",
                TestPki.PASSWORD,
                "ssl.truststore.type",
                "PKCS12",
                "ssl.truststore.location",
                pki.file("client-trust.p12").toString(),
                "ssl.truststore.password",
                TestPki.PASSWORD))) {
            admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return name;
    }

    @Test
    void aClientWithACertificateAndNoCredentialsSendsAndReceives() {
        String topic = topic();
        ResourceId subscription = SharedKafka.group(topic);
        try (ClusterConnection connection = connect(TlsProvider.of(TlsMaterial.of("client", pki.context(true))))) {
            Transport transport = connection.transport().orElseThrow();
            try (Sender sender =
                    transport.sender(SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults())) {
                sender.send(KafkaMessagingIT.message(5));
            }
            try (Receiver receiver = transport.receiver(subscription, Scope.whole(), ReceiverOptions.consume())) {
                List<Message> got = new ArrayList<>();
                await("the message", () -> {
                    got.addAll(receiver.receive(1, Duration.ofMillis(300)));
                    return !got.isEmpty();
                });
                assertThat(KafkaMessagingIT.seq(got.getFirst())).isEqualTo(5);
                receiver.settle(got.getFirst(), Disposition.accept());
            }
        }
    }

    @Test
    void newMaterialReachesTheNextConnectionWithoutReconnecting() {
        String topic = topic();
        ResourceId subscription = SharedKafka.group(topic);
        AtomicReference<TlsMaterial> current = new AtomicReference<>(TlsMaterial.of("anonymous", pki.context(false)));
        try (ClusterConnection connection = connect(() -> Optional.of(current.get()))) {
            Transport transport = connection.transport().orElseThrow();
            assertThatThrownBy(() -> transport.sender(
                            SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults()))
                    .isInstanceOf(BrokerException.class);

            current.set(TlsMaterial.of("client", pki.context(true)));
            try (Sender sender =
                    transport.sender(SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults())) {
                assertThat(sender.send(KafkaMessagingIT.message(1)).ref()).isPresent();
            }
        }
    }
}
