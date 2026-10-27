package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.artemis.ArtemisDriver;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.messaging.Transport;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.springframework.boot.artemis.autoconfigure.ArtemisConnectionDetails;
import org.springframework.boot.artemis.autoconfigure.ArtemisMode;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslStoreBundle;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class DerivedConnectionsTest {

    private final FakeDriver kafka = new FakeDriver(new KafkaDriver().system(), new KafkaDriver().schema());
    private final FakeDriver artemis = new FakeDriver(new ArtemisDriver().system(), new ArtemisDriver().schema());
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    SslAutoConfiguration.class, KafkaAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
            .withBean(BrokerDrivers.class, () -> BrokerDrivers.of(List.of(kafka, artemis)));

    private static final String SCRAM =
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"app\" password=\""
                    + TestSecrets.PASSWORD + "\";";

    private record ArtemisDetails(
            @Nullable ArtemisMode mode,
            @Nullable String url,
            @Nullable String user,
            @Nullable String password) implements ArtemisConnectionDetails {

        @Override
        public @Nullable ArtemisMode getMode() {
            return mode;
        }

        @Override
        public @Nullable String getBrokerUrl() {
            return url;
        }

        @Override
        public @Nullable String getUser() {
            return user;
        }

        @Override
        public @Nullable String getPassword() {
            return password;
        }
    }

    private static ArtemisDetails nativeArtemis() {
        return new ArtemisDetails(ArtemisMode.NATIVE, "tcp://broker:61616", "artemis", TestSecrets.PASSWORD);
    }

    @Test
    void kafkaFromSpringPropertiesNeedsNoOpenBrokerConfiguration() {
        runner.withPropertyValues("spring.kafka.bootstrap-servers=a:9092,b:9092")
                .run(context -> {
                    assertThat(context).hasBean("kafkaClusterConnection").hasBean("kafkaTransport");
                    assertThat(context).doesNotHaveBean("artemisClusterConnection");
                    FakeDriver.Request request = kafka.requests.getFirst();
                    assertThat(request.cluster().clusterEndpoints())
                            .containsExactly(
                                    new Endpoint(Plane.DATA, "kafka", "a:9092"),
                                    new Endpoint(Plane.DATA, "kafka", "b:9092"));
                    assertThat(context.getBean(ClusterConnection.class))
                            .isSameAs(context.getBean("kafkaClusterConnection"));
                });
    }

    @Test
    void kafkaTlsFollowsTheBundleSpringNames() {
        DefaultSslBundleRegistry registry = new DefaultSslBundleRegistry("broker", SslBundle.of(SslStoreBundle.NONE));
        runner.withPropertyValues(
                        "spring.kafka.bootstrap-servers=a:9092",
                        "spring.kafka.security.protocol=SSL",
                        "spring.kafka.ssl.bundle=broker")
                .withBean("sslBundleRegistry", DefaultSslBundleRegistry.class, () -> registry)
                .run(context -> {
                    FakeDriver.Request request = kafka.requests.getFirst();
                    assertThat(request.cluster().options()).containsEntry("security.protocol", "SSL");
                    AtomicInteger changes = new AtomicInteger();
                    request.options().tls().onChange(changes::incrementAndGet);
                    TlsMaterial before = request.options().tls().current().orElseThrow();

                    registry.updateBundle("broker", SslBundle.of(SslStoreBundle.NONE));

                    assertThat(changes).hasValue(1);
                    assertThat(request.options().tls().current().orElseThrow().id())
                            .isNotEqualTo(before.id());
                });
    }

    @Test
    void kafkaUsesTheDetailsBundleWhenNoBundleIsNamed() {
        SslBundle bundle = SslBundle.of(SslStoreBundle.NONE);
        KafkaConnectionDetails details = new KafkaConnectionDetails() {
            @Override
            public List<String> getBootstrapServers() {
                return List.of("c:9093");
            }

            @Override
            public SslBundle getSslBundle() {
                return bundle;
            }

            @Override
            public String getSecurityProtocol() {
                return "SSL";
            }
        };
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
                .withBean(BrokerDrivers.class, () -> BrokerDrivers.of(List.of(kafka, artemis)))
                .withBean(KafkaConnectionDetails.class, () -> details)
                .run(context -> {
                    FakeDriver.Request request = kafka.requests.getFirst();
                    assertThat(request.cluster().options()).containsEntry("security.protocol", "SSL");
                    TlsMaterial material = request.options().tls().current().orElseThrow();
                    assertThat(material.id()).startsWith("ssl-bundle@");
                });
    }

    @Test
    void scramCredentialsComeFromSpringsJaasConfigAndNeverAppearInATextForm() {
        runner.withPropertyValues(
                        "spring.kafka.bootstrap-servers=a:9092",
                        "spring.kafka.security.protocol=SASL_PLAINTEXT",
                        "spring.kafka.properties.sasl.mechanism=SCRAM-SHA-512",
                        "spring.kafka.properties.sasl.jaas.config=" + SCRAM)
                .run(context -> {
                    FakeDriver.Request request = kafka.requests.getFirst();
                    assertThat(request.cluster().options())
                            .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                            .containsEntry("security.protocol", "SASL_PLAINTEXT");
                    assertThat(request.credentials().forPlane(Plane.DATA))
                            .isEqualTo(Credentials.usernamePassword("app", TestSecrets.PASSWORD));
                    assertThat(context.getBean(OpenBrokerConnections.class)
                                    .connection("kafka")
                                    .toString())
                            .doesNotContain(TestSecrets.PASSWORD);
                });
    }

    @Test
    void aMechanismThatCannotBeDerivedFailsTheStartUnlessCredentialsAreGiven() {
        ApplicationContextRunner oauth = runner.withPropertyValues(
                "spring.kafka.bootstrap-servers=a:9092",
                "spring.kafka.properties.sasl.mechanism=OAUTHBEARER",
                "spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientSecret=\""
                        + TestSecrets.BAD + "\";");

        oauth.run(context -> {
            assertThat(context).hasFailed();
            String description = new ConnectionFailureAnalyzer()
                    .analyze(context.getStartupFailure())
                    .getDescription();
            assertThat(description)
                    .contains("OAUTHBEARER")
                    .contains("credentials")
                    .contains("CredentialSource");
            assertThat(description).doesNotContain(TestSecrets.BAD);
        });
        oauth.withPropertyValues(
                        "openbroker.connections.kafka.credentials.form=bearer-token",
                        "openbroker.connections.kafka.credentials.token=" + TestSecrets.TOKEN,
                        "openbroker.connections.kafka.credentials.expires-at=2099-01-01T00:00:00Z")
                .run(context -> assertThat(context).hasNotFailed());
        oauth.withBean("kafka", CredentialSource.class, CredentialSource::none)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void theMechanismIsComparedWithoutCaseAndStoredInUpperCase() {
        runner.withPropertyValues(
                        "spring.kafka.bootstrap-servers=a:9092",
                        "spring.kafka.properties.sasl.mechanism=scram-sha-256",
                        "spring.kafka.properties.sasl.jaas.config=" + SCRAM)
                .run(context -> assertThat(kafka.requests.getFirst().cluster().options())
                        .containsEntry("sasl.mechanism", "SCRAM-SHA-256"));
    }

    @Test
    void aMechanismTheDriverDoesNotKnowIsNotDerivedAndFailsTheStartWithAnAdvice() {
        for (String mechanism : List.of("GSSAPI", "AWS_MSK_IAM")) {
            runner.withPropertyValues(
                            "spring.kafka.bootstrap-servers=a:9092",
                            "spring.kafka.properties.sasl.mechanism=" + mechanism)
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure())
                                .hasRootCauseInstanceOf(ConnectionConfigurationException.class);
                        assertThat(new ConnectionFailureAnalyzer()
                                        .analyze(context.getStartupFailure())
                                        .getDescription())
                                .contains(mechanism)
                                .contains("does not support")
                                .contains("CredentialSource");
                        assertThat(kafka.requests).isEmpty();
                    });
        }
    }

    @Test
    void artemisFromAServiceConnectionLikeDetailsDerivesCoreEndpointAndDataCredentials() {
        runner.withBean(ArtemisConnectionDetails.class, ArtemisDetailsFactory::nativeDetails)
                .run(context -> {
                    assertThat(context).hasBean("artemisClusterConnection").hasBean("artemisTransport");
                    FakeDriver.Request request = artemis.requests.getFirst();
                    assertThat(request.cluster().nodes()).hasSize(1);
                    assertThat(request.cluster().nodes().getFirst().id()).isEqualTo(NodeId.of("artemis"));
                    assertThat(request.cluster().nodes().getFirst().endpoints())
                            .containsExactly(new Endpoint(Plane.DATA, "core", "tcp://broker:61616"));
                    assertThat(request.credentials().forPlane(Plane.DATA))
                            .isEqualTo(Credentials.usernamePassword("artemis", TestSecrets.PASSWORD));
                    assertThat(request.credentials().forPlane(Plane.MANAGEMENT)).isEqualTo(new Credentials.None());
                });
    }

    @Test
    void embeddedArtemisIsNotDerived() {
        runner.withBean(
                        ArtemisConnectionDetails.class,
                        () -> new ArtemisDetails(ArtemisMode.EMBEDDED, null, null, null))
                .run(context -> {
                    assertThat(context.getBean(OpenBrokerConnections.class).names())
                            .doesNotContain("artemis");
                    assertThat(artemis.requests).isEmpty();
                });
        runner.withBean(ArtemisConnectionDetails.class, ArtemisDetailsFactory::nativeDetails)
                .withPropertyValues("spring.artemis.mode=embedded")
                .run(context -> {
                    assertThat(context).doesNotHaveBean("artemisClusterConnection");
                    assertThat(artemis.requests).isEmpty();
                });
    }

    @Test
    void anOverlayAddsWhatSpringCannotSay() {
        runner.withBean(ArtemisConnectionDetails.class, ArtemisDetailsFactory::nativeDetails)
                .withPropertyValues(
                        "openbroker.connections.artemis.cluster.nodes[0].id=artemis",
                        "openbroker.connections.artemis.cluster.nodes[0].endpoints[0].plane=management",
                        "openbroker.connections.artemis.cluster.nodes[0].endpoints[0].protocol=jolokia",
                        "openbroker.connections.artemis.cluster.nodes[0].endpoints[0].address=http://broker:8161/console/jolokia",
                        "openbroker.connections.artemis.management-credentials.form=username-password",
                        "openbroker.connections.artemis.management-credentials.username=console",
                        "openbroker.connections.artemis.management-credentials.password=" + TestSecrets.OTHER_PASSWORD)
                .run(context -> {
                    FakeDriver.Request request = artemis.requests.getFirst();
                    assertThat(request.cluster().nodes()).hasSize(1);
                    assertThat(request.cluster().nodes().getFirst().endpoints())
                            .extracting(Endpoint::protocol)
                            .containsExactlyInAnyOrder("core", "jolokia");
                    assertThat(request.credentials().forPlane(Plane.DATA))
                            .isEqualTo(Credentials.usernamePassword("artemis", TestSecrets.PASSWORD));
                    assertThat(request.credentials().forPlane(Plane.MANAGEMENT))
                            .isEqualTo(Credentials.usernamePassword("console", TestSecrets.OTHER_PASSWORD));
                });
    }

    @Test
    void derivationCanBeTurnedOff() {
        runner.withBean(ArtemisConnectionDetails.class, ArtemisDetailsFactory::nativeDetails)
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=a:9092", "openbroker.spring-connections.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ClusterConnection.class).doesNotHaveBean(Transport.class);
                    assertThat(kafka.requests).isEmpty();
                    assertThat(artemis.requests).isEmpty();
                });
    }

    @Test
    void withoutBootsKafkaClassesNothingIsDerived() {
        runner.withClassLoader(new FilteredClassLoader(KafkaConnectionDetails.class))
                .withBean(ArtemisConnectionDetails.class, ArtemisDetailsFactory::nativeDetails)
                .run(context -> {
                    assertThat(context).hasNotFailed().hasBean("artemisClusterConnection");
                    assertThat(context).doesNotHaveBean("kafkaClusterConnection");
                });
    }

    /** Test details are made apart so a bean supplier can name them. */
    static final class ArtemisDetailsFactory {
        static ArtemisConnectionDetails nativeDetails() {
            return nativeArtemis();
        }
    }
}
