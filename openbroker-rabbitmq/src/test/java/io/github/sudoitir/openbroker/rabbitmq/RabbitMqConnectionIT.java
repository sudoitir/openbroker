package io.github.sudoitir.openbroker.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.ConnectionFactory;
import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.ManagementClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The connection against the real three-node cluster: verify, what it refuses, and what it unwraps. */
class RabbitMqConnectionIT {

    private static RabbitMqTestCluster cluster;
    private static BrokerDrivers drivers;

    @BeforeAll
    static void start() {
        cluster = RabbitMqTestCluster.shared();
        drivers = BrokerDrivers.of(List.of(new RabbitMqDriver()));
    }

    @AfterAll
    static void stop() {
        drivers.close();
    }

    private static ConnectRequest request(ClusterSpec spec, Credentials credentials) {
        return new ConnectRequest(
                RabbitMqDriver.SYSTEM_ID, spec, CredentialSource.of(credentials), ConnectOptions.defaults());
    }

    @Test
    void verifyDescribesEveryNodeAndProbesTheAccount() {
        ConnectionReport report = drivers.verify(request(cluster.spec(), cluster.credentials()));

        assertThat(report.nodes().failures()).isEmpty();
        assertThat(report.nodes().value()).extracting(NodeDescription::nativeId).hasSize(3);
        assertThat(report.access().of(Capability.MANAGEMENT_READ).status()).isEqualTo(Assessment.Status.AVAILABLE);
    }

    @Test
    void aConnectionWithoutADataEndpointStillReadsAndOffersNoTransport() {
        try (ClusterConnection connection =
                drivers.connect(request(cluster.managementOnlySpec(), cluster.credentials()))) {
            assertThat(connection.transport()).isEmpty();
            assertThat(connection.notifications()).isPresent();
            assertThat(connection.unwrap(ConnectionFactory.class)).isEmpty();
            assertThat(connection.unwrap(ManagementClient.class)).isPresent();
            assertThat(connection.nodes().describe(Scope.whole()).value()).hasSize(3);
        }
    }

    @Test
    void aConnectionWithADataEndpointUnwrapsTheAmqpFactory() {
        try (ClusterConnection connection = drivers.connect(request(cluster.spec(), cluster.credentials()))) {
            assertThat(connection.transport()).isPresent();
            assertThat(connection.unwrap(ConnectionFactory.class)).isPresent();
            assertThat(connection.unwrap(String.class)).isEmpty();
        }
    }

    @Test
    void anEndpointThatIsNotRabbitMqIsRefused() {
        // The management port of the first node answers HTTP, so a data address as management is not RabbitMQ.
        String amqpAddress = cluster.amqpAddress(0);
        ClusterSpec notRabbit = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, "http://" + amqpAddress)),
                Map.of());

        ConnectRequest request = request(notRabbit, cluster.credentials());
        assertThatThrownBy(() -> drivers.verify(request)).isInstanceOf(BrokerException.class);
    }

    @Test
    void wrongCredentialsAreUnauthenticated() {
        ConnectRequest request =
                request(cluster.spec(), Credentials.usernamePassword(RabbitMqTestCluster.ADMIN_USER, "wrong"));
        assertThatThrownBy(() -> drivers.verify(request))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED));
    }
}
