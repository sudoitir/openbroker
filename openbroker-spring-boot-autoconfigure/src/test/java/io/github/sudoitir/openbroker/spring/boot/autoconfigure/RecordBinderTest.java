package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RecordBinderTest {

    private static Connection bind(String yaml) {
        return new RecordBinder(Yaml.environment(yaml), "orders")
                .bind("openbroker.connections.orders", Connection.class, Connection.defaults());
    }

    @Test
    void bindsAFullKafkaConnection() {
        Connection connection = bind("""
                openbroker:
                  connections:
                    orders:
                      system: kafka
                      cluster:
                        cluster-endpoints:
                          - {plane: data, protocol: kafka, address: "a:9092"}
                          - {plane: data, protocol: kafka, address: "b:9092"}
                        options:
                          security.protocol: SASL_SSL
                          sasl.mechanism: SCRAM-SHA-512
                      credentials:
                        form: username-password
                        username: app
                        password: secret
                      options:
                        connect-timeout: 5s
                        call-timeout: 20s
                        max-response-bytes: 1024
                        ssl-bundle: kafka
                        spool-directory: /var/spool/ob
                        verify-hostname: false
                      sender:
                        guarantee: at-most-once
                        buffer: {max-in-flight: 5, max-bytes: 4096, max-block: 2s}
                      receiver:
                        flow: {max-unsettled: 7}
                      processor:
                        concurrency: 3
                        stop-grace: 5s
                        retry: {mode: managed, max-attempts: 4, backoff: {initial: 50ms, multiplier: 3, max: 2s, jitter: 0.5}}
                """);

        assertThat(connection.system()).isEqualTo("kafka");
        assertThat(connection.cluster().clusterEndpoints())
                .containsExactly(
                        new Endpoint(Plane.DATA, "kafka", "a:9092"), new Endpoint(Plane.DATA, "kafka", "b:9092"));
        assertThat(connection.cluster().options())
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512");
        assertThat(connection.credentials().toCredentials("orders", "credentials"))
                .isEqualTo(Credentials.usernamePassword("app", "secret"));
        assertThat(connection.options().connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(connection.options().callTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(connection.options().maxResponseBytes()).isEqualTo(1024L);
        assertThat(connection.options().sslBundle()).isEqualTo("kafka");
        assertThat(connection.options().spoolDirectory()).isEqualTo(Path.of("/var/spool/ob"));
        assertThat(connection.options().hostnameVerified()).isFalse();
        assertThat(connection.sender().guarantee()).isEqualTo(Guarantee.AT_MOST_ONCE);
        assertThat(connection.sender().buffer().maxInFlight()).isEqualTo(5);
        assertThat(connection.sender().buffer().maxBlock()).isEqualTo(Duration.ofSeconds(2));
        assertThat(connection.receiver().flow().maxUnsettled()).isEqualTo(7);
        assertThat(connection.processor().concurrency()).isEqualTo(3);
        assertThat(connection.processor().stopGrace()).isEqualTo(Duration.ofSeconds(5));
        assertThat(connection.processor().retry().maxAttempts()).isEqualTo(4);
        assertThat(connection.processor().retry().backoff().initial()).isEqualTo(Duration.ofMillis(50));
        assertThat(connection.processor().retry().backoff().jitter()).isEqualTo(0.5);
    }

    @Test
    void bindsAFullArtemisConnectionWithNodes() {
        Connection connection = bind("""
                openbroker:
                  connections:
                    orders:
                      system: artemis
                      cluster:
                        nodes:
                          - id: primary
                            endpoints:
                              - {plane: management, protocol: jolokia, address: "http://h:8161/console/jolokia"}
                              - {plane: data, protocol: core, address: "tcp://h:61616"}
                      credentials: {form: bearer-token, token: abc, expires-at: "2099-01-01T00:00:00Z"}
                """);

        assertThat(connection.cluster().nodes()).hasSize(1);
        assertThat(connection.cluster().nodes().getFirst().id()).isEqualTo(NodeId.of("primary"));
        assertThat(connection.cluster().nodes().getFirst().endpoints()).hasSize(2);
        assertThat(connection.credentials().toCredentials("orders", "credentials"))
                .isInstanceOf(Credentials.BearerToken.class);
    }

    @Test
    void aPartialBlockKeepsEveryOtherDefault() {
        Connection connection = bind("""
                openbroker:
                  connections:
                    orders:
                      system: kafka
                      sender:
                        guarantee: transacted
                """);

        assertThat(connection.sender()).isEqualTo(SenderOptions.defaults().withGuarantee(Guarantee.TRANSACTED));
        assertThat(connection.processor()).isEqualTo(Connection.defaults().processor());
        assertThat(connection.receiver()).isEqualTo(Connection.defaults().receiver());
    }

    @Test
    void anOptionalComponentWraps() {
        Connection connection = bind("""
                openbroker:
                  connections:
                    orders:
                      processor:
                        receive:
                          filter: {dialect: sql92, expression: "a = 1"}
                        retry: {backoff: {initial: 1s, max: 5s}}
                        ordering: subscription
                        concurrency: 1
                """);

        assertThat(connection.processor().receive().filter()).isPresent();
        assertThat(connection.processor().ordering()).isEqualTo(OrderingScope.SUBSCRIPTION);
        assertThat(connection.processor().retry().backoff().multiplier()).isEqualTo(2.0);
    }

    @Test
    void rejectsAValueOutOfRangeNamingTheKeyAndTheLimit() {
        assertThatThrownBy(() -> bind("""
                        openbroker:
                          connections:
                            orders:
                              processor:
                                concurrency: 1000
                        """))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining("openbroker.connections.orders.processor.concurrency")
                .hasMessageContaining("64");
    }

    @Test
    void rejectsADeadLetterTargetAsCodeOnly() {
        assertThatThrownBy(() -> bind("""
                        openbroker:
                          connections:
                            orders:
                              processor:
                                retry:
                                  dead-letter:
                                    destination: orders.dlq
                        """))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("openbroker.connections.orders.processor.retry.dead-letter")
                .hasMessageContaining("set it in code");
    }

    @Test
    void rejectsAWrongTypeWithoutQuotingTheValue() {
        assertThatThrownBy(() -> bind("""
                        openbroker:
                          connections:
                            orders:
                              sender:
                                guarantee: nonsense-value
                        """))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("openbroker.connections.orders.sender.guarantee")
                .hasMessageContaining("at-least-once")
                .hasMessageNotContaining("nonsense-value");
    }

    @Test
    void anEndpointNeedsAllItsParts() {
        assertThatThrownBy(() -> bind("""
                        openbroker:
                          connections:
                            orders:
                              cluster:
                                cluster-endpoints:
                                  - {plane: data, address: "a:9092"}
                        """))
                .isInstanceOf(ConnectionConfigurationException.class)
                .hasMessageContaining("cluster.cluster-endpoints[0].protocol")
                .hasMessageContaining("required");
    }

    @Test
    void listsTheBindableKeyPaths() {
        assertThat(RecordBinder.keyPaths(Connection.class))
                .contains(
                        "system",
                        "cluster.options",
                        "cluster.nodes[].endpoints[].address",
                        "sender.buffer.max-block",
                        "processor.retry.backoff.jitter",
                        "processor.receive.filter.dialect",
                        "credentials.form")
                .doesNotContain("processor.retry.dead-letter");
    }
}
