package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.diagnostics.FailureAnalysis;

class ConnectionBinderTest {

    private static final BrokerDrivers DRIVERS = BrokerDrivers.of(List.of(new FakeDriver()));

    private static Connection bind(String yaml, Connection derived) {
        return new ConnectionBinder(Yaml.environment(yaml), DRIVERS).bind("orders", derived);
    }

    @Test
    void aMisspeltOptionNamesTheConnectionAndTheOption() {
        assertThatThrownBy(() -> bind("""
                        openbroker.connections.orders:
                          system: fake
                          cluster.options.moode: a
                        """, null))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining("openbroker.connections.orders.cluster.options.moode");
    }

    @Test
    void aMissingDriverNamesTheSystem() {
        assertThatThrownBy(() -> bind("""
                        openbroker.connections.orders:
                          system: pulsar
                        """, null))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("pulsar")
                .hasMessageContaining("[fake]");
    }

    @Test
    void aMissingSystemIsRefused() {
        assertThatThrownBy(() -> bind("openbroker.connections.orders.cluster.options.mode: a", null))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("openbroker.connections.orders.system");
    }

    @Test
    void aSensitiveOptionOfTheWrongTypeNamesTheKeyAndNeverTheValue() {
        assertThatThrownBy(() -> bind("""
                        openbroker.connections.orders:
                          system: fake
                          cluster.options.pin: ${test.bad}
                        """, null))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("openbroker.connections.orders.cluster.options.pin")
                .hasMessageNotContaining(TestSecrets.BAD);
    }

    @Test
    void aValueNotAllowedNamesWhatIs() {
        assertThatThrownBy(() -> bind("""
                        openbroker.connections.orders:
                          system: fake
                          cluster.options.mode: c
                        """, null)).hasMessageContaining("[a, b]");
    }

    @Test
    void theFailureAnalyzerReportsTheMessageAndTheKey() {
        ConnectionConfigurationException failure =
                new ConnectionConfigurationException("orders", "openbroker.connections.orders.system", "is required");

        FailureAnalysis analysis = new ConnectionFailureAnalyzer().analyze(new IllegalStateException(failure));

        assertThat(analysis.getDescription()).contains("orders").contains("is required");
        assertThat(analysis.getAction()).contains("openbroker.connections.orders.system");
    }

    @Test
    void anOverlayLaysConfigurationOverTheDerivedConnection() {
        Connection derived = new Connection(
                FakeDriver.SYSTEM,
                new ClusterSpec(
                        List.of(new NodeSpec(NodeId.of("n"), List.of(new Endpoint(Plane.DATA, "core", "tcp://h:1")))),
                        List.of(),
                        Map.of("mode", "a")),
                CredentialsProperties.unset(),
                CredentialsProperties.unset(),
                ConnectionOptions.unset(),
                Connection.defaults().sender(),
                Connection.defaults().receiver(),
                Connection.defaults().processor());

        Connection bound = bind("""
                openbroker.connections.orders:
                  cluster:
                    nodes:
                      - id: n
                        endpoints:
                          - {plane: management, protocol: jolokia, address: "http://h:8161/console/jolokia"}
                    options.pin: 1
                  processor.concurrency: 2
                """, derived);

        assertThat(bound.system()).isEqualTo(FakeDriver.SYSTEM);
        assertThat(bound.cluster().nodes()).hasSize(1);
        assertThat(bound.cluster().nodes().getFirst().endpoints())
                .extracting(Endpoint::protocol)
                .containsExactlyInAnyOrder("core", "jolokia");
        assertThat(bound.cluster().options()).containsEntry("mode", "a").containsEntry("pin", "1");
        assertThat(bound.processor().concurrency()).isEqualTo(2);
    }
}
