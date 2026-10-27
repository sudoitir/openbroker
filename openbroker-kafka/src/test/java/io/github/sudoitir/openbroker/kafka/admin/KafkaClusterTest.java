package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.security.JaasContext;
import org.junit.jupiter.api.Test;

class KafkaClusterTest {

    private static ClusterSpec spec(Map<String, String> options, String... bootstrap) {
        return new ClusterSpec(
                List.of(),
                Arrays.stream(bootstrap)
                        .map(a -> Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, a))
                        .toList(),
                options);
    }

    private static DriverContext context(ConnectOptions options) {
        return new DriverContext() {
            @Override
            public ConnectOptions options() {
                return options;
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
    }

    private static KafkaCluster cluster(ClusterSpec spec, CredentialSource credentials, ConnectOptions options) {
        return new KafkaCluster(spec, credentials, context(options));
    }

    @Test
    void bootstrapServersAreTheDataEndpointsOfTheClusterAndItsNodes() {
        ClusterSpec spec = new ClusterSpec(
                List.of(new NodeSpec(
                        NodeId.of("a"), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "node-a:9092")))),
                List.of(
                        Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "one:9092"),
                        Endpoint.of(Plane.MANAGEMENT, "jolokia", "http://ignored")),
                Map.of());

        try (KafkaCluster cluster = cluster(spec, CredentialSource.none(), ConnectOptions.defaults())) {
            assertThat(cluster.bootstrapServers()).isEqualTo("one:9092,node-a:9092");
        }
    }

    @Test
    void aClusterWithoutBootstrapServersIsRefusedBeforeAnythingIsSent() {
        try (KafkaCluster cluster = cluster(spec(Map.of()), CredentialSource.none(), ConnectOptions.defaults())) {
            assertThatThrownBy(cluster::bootstrapServers).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                assertThat(e.reason()).isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
            });
        }
    }

    @Test
    void plaintextClientsCarryNoLogin() {
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.CLIENT_ID, "console"), "one:9092"),
                CredentialSource.of(Credentials.usernamePassword("u", "p")),
                ConnectOptions.defaults())) {
            Map<String, Object> config = cluster.clientConfig(Plane.MANAGEMENT);

            assertThat(config)
                    .containsEntry("security.protocol", "PLAINTEXT")
                    .containsEntry("client.id", "console")
                    .doesNotContainKey("sasl.jaas.config");
        }
    }

    @Test
    void saslLogsInWithThePlanesOwnCredentials() {
        CredentialSource credentials = plane -> plane == Plane.MANAGEMENT
                ? Credentials.usernamePassword("admin", "s3cret")
                : Credentials.usernamePassword("app", "other");
        try (KafkaCluster cluster = cluster(
                spec(
                        Map.of(
                                KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_PLAINTEXT,
                                KafkaOptions.SASL_MECHANISM, KafkaOptions.SCRAM_SHA_512),
                        "one:9092"),
                credentials,
                ConnectOptions.defaults())) {
            Map<String, Object> admin = cluster.clientConfig(Plane.MANAGEMENT);
            Map<String, Object> data = cluster.clientConfig(Plane.DATA);

            assertThat(admin).containsEntry("sasl.mechanism", "SCRAM-SHA-512");
            assertThat(login(admin)).containsEntry("username", "admin").containsEntry("password", "s3cret");
            assertThat(login(data)).containsEntry("username", "app");
        }
    }

    @Test
    void aPasswordWithQuotesSurvivesTheLoginConfiguration() {
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_PLAINTEXT), "one:9092"),
                CredentialSource.of(Credentials.usernamePassword("us\"er", "pa\\ss\"word")),
                ConnectOptions.defaults())) {
            assertThat(login(cluster.clientConfig(Plane.MANAGEMENT)))
                    .containsEntry("username", "us\"er")
                    .containsEntry("password", "pa\\ss\"word");
        }
    }

    @Test
    void theLoginNeverShowsInAPrintedConfig() {
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_PLAINTEXT), "one:9092"),
                CredentialSource.of(Credentials.usernamePassword("u", "s3cret")),
                ConnectOptions.defaults())) {
            assertThat(cluster.clientConfig(Plane.MANAGEMENT).toString()).doesNotContain("s3cret");
        }
    }

    @Test
    void saslWithoutAPasswordIsRefusedWithoutQuotingAnything() {
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_SSL), "one:9092"),
                CredentialSource.none(),
                ConnectOptions.defaults())) {
            assertThatThrownBy(() -> cluster.clientConfig(Plane.MANAGEMENT))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                        assertThat(e.getMessage()).contains("user name and password");
                    });
        }
    }

    @Test
    void anUnknownSaslMechanismIsRefused() {
        try (KafkaCluster cluster = cluster(
                spec(
                        Map.of(
                                KafkaOptions.SECURITY_PROTOCOL,
                                KafkaOptions.SASL_PLAINTEXT,
                                KafkaOptions.SASL_MECHANISM,
                                "GSSAPI"),
                        "one:9092"),
                CredentialSource.of(Credentials.usernamePassword("u", "topsecret")),
                ConnectOptions.defaults())) {
            assertThatThrownBy(() -> cluster.clientConfig(Plane.MANAGEMENT))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                        assertThat(e.getMessage()).contains("GSSAPI").doesNotContain("topsecret");
                    });
        }
    }

    @Test
    void callerSuppliedTlsMaterialIsRefusedNotIgnored() throws Exception {
        ConnectOptions options =
                ConnectOptions.defaults().withTls(TlsProvider.of(TlsMaterial.of("m", SSLContext.getDefault())));
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SSL), "one:9092"),
                CredentialSource.none(),
                options)) {
            assertThatThrownBy(() -> cluster.clientConfig(Plane.MANAGEMENT))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        }
    }

    @Test
    void theCallTimeoutBecomesTheClientsRequestTimeout() {
        try (KafkaCluster cluster =
                cluster(spec(Map.of(), "one:9092"), CredentialSource.none(), ConnectOptions.defaults())) {
            assertThat(cluster.clientConfig(Plane.MANAGEMENT))
                    .containsEntry("request.timeout.ms", 10_000)
                    .containsEntry("socket.connection.setup.timeout.ms", 3_000);
        }
    }

    private static Map<String, String> login(Map<String, Object> config) {
        var entries = JaasContext.loadClientContext(config).configurationEntries();
        return entries.getFirst().getOptions().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
    }
}
