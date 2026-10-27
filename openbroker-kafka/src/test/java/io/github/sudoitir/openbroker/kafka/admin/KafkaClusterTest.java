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
import io.github.sudoitir.openbroker.kafka.security.CredentialCallbackHandler;
import io.github.sudoitir.openbroker.kafka.security.TlsEngineFactory;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
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

    private static String nameFor(Map<String, Object> config, String mechanism) throws Exception {
        CredentialCallbackHandler handler = new CredentialCallbackHandler();
        handler.configure(config, mechanism, List.of());
        NameCallback name = new NameCallback("user");
        PasswordCallback password = new PasswordCallback("password", false);
        handler.handle(new Callback[] {name, password});
        return name.getName() + ":" + new String(password.getPassword());
    }

    @Test
    void saslAsksTheSourceForThePlanesOwnCredentialsAtEveryAuthentication() throws Exception {
        AtomicReference<String> rotating = new AtomicReference<>("s3cret");
        CredentialSource credentials = plane -> plane == Plane.MANAGEMENT
                ? Credentials.usernamePassword("admin", rotating.get())
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

            assertThat(admin)
                    .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                    .containsEntry("sasl.client.callback.handler.class", CredentialCallbackHandler.class);
            assertThat(admin.get("sasl.jaas.config").toString())
                    .doesNotContain("admin")
                    .doesNotContain("s3cret");
            assertThat(nameFor(admin, "SCRAM-SHA-512")).isEqualTo("admin:s3cret");
            assertThat(nameFor(data, "SCRAM-SHA-512")).isEqualTo("app:other");

            rotating.set("rotated");
            assertThat(nameFor(admin, "SCRAM-SHA-512")).isEqualTo("admin:rotated");
        }
    }

    @Test
    void aBearerTokenSourceLogsInThroughTheTokenHandler() throws Exception {
        Instant expires = Instant.now().plusSeconds(60);
        try (KafkaCluster cluster = cluster(
                spec(
                        Map.of(
                                KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_SSL,
                                KafkaOptions.SASL_MECHANISM, KafkaOptions.OAUTHBEARER),
                        "one:9092"),
                CredentialSource.of(Credentials.bearerToken("tok3n", expires)),
                ConnectOptions.defaults())) {
            Map<String, Object> config = cluster.clientConfig(Plane.DATA);
            assertThat(config)
                    .containsEntry("sasl.login.callback.handler.class", CredentialCallbackHandler.class)
                    .containsEntry("sasl.client.callback.handler.class", CredentialCallbackHandler.class);

            CredentialCallbackHandler handler = new CredentialCallbackHandler();
            handler.configure(config, KafkaOptions.OAUTHBEARER, List.of());
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[] {callback});
            assertThat(callback.token().value()).isEqualTo("tok3n");
            assertThat(callback.token().lifetimeMs()).isEqualTo(expires.toEpochMilli());
            assertThat(callback.token().toString()).doesNotContain("tok3n");
        }
    }

    @Test
    void aPasswordWhereATokenIsNeededIsRefusedNamingTheForm() {
        try (KafkaCluster cluster = cluster(
                spec(
                        Map.of(
                                KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_SSL,
                                KafkaOptions.SASL_MECHANISM, KafkaOptions.OAUTHBEARER),
                        "one:9092"),
                CredentialSource.of(Credentials.usernamePassword("u", "topsecret")),
                ConnectOptions.defaults())) {
            assertThatThrownBy(() -> cluster.clientConfig(Plane.MANAGEMENT))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                        assertThat(e.getMessage()).contains("bearer token").doesNotContain("topsecret");
                    });
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
    void tlsMaterialIsAskedForAtEveryNewConnectionAndTheHostNameIsAlwaysVerified() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        SSLContext context = SSLContext.getDefault();
        TlsProvider provider = () -> {
            asked.incrementAndGet();
            return Optional.of(new TlsMaterial("m", context, false, List.of(), List.of()));
        };
        try (KafkaCluster cluster = cluster(
                spec(Map.of(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SSL), "one:9092"),
                CredentialSource.none(),
                ConnectOptions.defaults().withTls(provider))) {
            Map<String, Object> config = cluster.clientConfig(Plane.MANAGEMENT);
            assertThat(config).containsEntry("ssl.engine.factory.class", TlsEngineFactory.class);

            TlsEngineFactory factory = new TlsEngineFactory();
            factory.configure(config);
            for (int connection = 1; connection <= 2; connection++) {
                SSLEngine engine = factory.createClientSslEngine("broker-1", 9093, "https");
                assertThat(engine.getUseClientMode()).isTrue();
                assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm())
                        .as("verification stays on although the material says off")
                        .isEqualToIgnoringCase("https");
                assertThat(asked).hasValue(connection);
            }
            assertThat(factory.createClientSslEngine("broker-1", 9093, "")
                            .getSSLParameters()
                            .getEndpointIdentificationAlgorithm())
                    .isEqualToIgnoringCase("https");
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
}
