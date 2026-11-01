package io.github.sudoitir.openbroker.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The driver over TLS: https for the management API and amqps for messages, against one node with TLS listeners. */
class RabbitMqTlsIT {

    private static TestPki pki;
    private static RabbitMqTestCluster cluster;
    private static BrokerDrivers drivers;

    @BeforeAll
    static void start() {
        pki = new TestPki();
        cluster = RabbitMqTestCluster.singleTls(pki);
        drivers = BrokerDrivers.of(List.of(new RabbitMqDriver()));
    }

    @AfterAll
    static void stop() {
        drivers.close();
        cluster.close();
    }

    private static ConnectRequest request(ConnectOptions options) {
        return new ConnectRequest(
                RabbitMqDriver.SYSTEM, cluster.tlsSpec(), CredentialSource.of(cluster.credentials()), options);
    }

    private static ConnectOptions trusting() {
        return ConnectOptions.defaults().withTls(TlsProvider.of(TlsMaterial.of("pki", pki.context(false))));
    }

    @Test
    void verifyReachesTheClusterOverHttps() {
        ConnectionReport report = drivers.verify(request(trusting()));

        assertThat(report.nodes().failures()).isEmpty();
        assertThat(report.nodes().value()).hasSize(1);
        assertThat(report.access().of(Capability.MANAGEMENT_READ).status()).isEqualTo(Assessment.Status.AVAILABLE);
    }

    @Test
    void aMessageIsSentOverAmqps() {
        String name = "tls-" + UUID.randomUUID().toString().substring(0, 8);
        ResourceId queue = ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", name);
        RabbitMq admin = RabbitMq.of(cluster, 0);
        admin.declareQueue("/", name, "quorum");
        try (ClusterConnection connection = drivers.connect(request(trusting()));
                Sender sender = connection
                        .transport()
                        .orElseThrow()
                        .sender(SendTarget.subscription(queue), Scope.whole(), SenderOptions.defaults())) {
            sender.send(new OutboundMessage(
                    MessageHeaders.empty(), Map.of(), MessageBody.of("secure".getBytes()), Map.of()));

            assertThat(RabbitMq.eventually(Duration.ofSeconds(60), () -> admin.messagesReady("/", name), n -> n == 1))
                    .isEqualTo(1);
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void anHttpsEndpointWithoutTlsMaterialIsRefused() {
        assertThatThrownBy(() -> drivers.verify(request(ConnectOptions.defaults())))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }
}
