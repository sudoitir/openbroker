package io.github.sudoitir.openbroker.rabbitmq.amqp;

import static io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpTestClusters.cluster;
import static io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpTestClusters.data;
import static io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpTestClusters.node;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import java.time.Instant;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

class AmqpFactoryTest {

    private static final Credentials USER = Credentials.usernamePassword("ops", "s3cret");

    private static ConnectOptions withTls(boolean verify, List<String> protocols, List<String> ciphers)
            throws Exception {
        TlsMaterial material = new TlsMaterial("m", SSLContext.getDefault(), verify, protocols, ciphers);
        return ConnectOptions.defaults().withTls(TlsProvider.of(material));
    }

    @Test
    void theAddressesAreTheDataEndpointsClusterFirstThenNodes() {
        AmqpFactory factory = new AmqpFactory(cluster(
                USER,
                ConnectOptions.defaults(),
                List.of(
                        data("amqp", "rabbit.example.com"),
                        Endpoint.of(Plane.MANAGEMENT, "http", "http://rabbit.example.com:15672")),
                List.of(node("a", data("amqp", "a.example.com:5673")), node("b", data("amqp", "[::1]:5674")))));

        assertThat(factory.addresses())
                .containsExactly(
                        new Address("rabbit.example.com", 5672),
                        new Address("a.example.com", 5673),
                        new Address("::1", 5674));
    }

    @Test
    void aSpecWithoutADataEndpointIsRefused() {
        AmqpFactory factory = new AmqpFactory(cluster(
                USER,
                ConnectOptions.defaults(),
                Endpoint.of(Plane.MANAGEMENT, "http", "http://rabbit.example.com:15672")));

        for (Runnable call : new Runnable[] {factory::addresses, () -> factory.factory("/")}) {
            assertThatThrownBy(call::run).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                assertThat(e).hasMessageContaining("no DATA endpoint");
            });
        }
    }

    @Test
    void theFactoryHasRecoveryOffAndNamesTheConnection() {
        AmqpFactory factory = new AmqpFactory(cluster(USER, ConnectOptions.defaults(), data("amqp", "h:5672")));

        ConnectionFactory built = factory.factory("orders");

        assertThat(built.isAutomaticRecoveryEnabled()).isFalse();
        assertThat(built.isTopologyRecoveryEnabled()).isFalse();
        assertThat(built.getVirtualHost()).isEqualTo("orders");
        assertThat(built.getUsername()).isEqualTo("ops");
        assertThat(built.getPassword()).isEqualTo("s3cret");
        assertThat(built.isSSL()).isFalse();
        assertThat(built.getRequestedHeartbeat()).isPositive();
        assertThat(built.getClientProperties()).containsEntry("connection_name", factory.name("orders"));
        assertThat(factory.name("orders")).matches("openbroker-[0-9a-f]{8}-orders");
    }

    @Test
    void aBearerTokenIsThePasswordOfAnEmptyUser() {
        AmqpFactory factory = new AmqpFactory(cluster(
                Credentials.bearerToken("tok", Instant.now().plusSeconds(60)),
                ConnectOptions.defaults(),
                data("amqp", "h")));

        ConnectionFactory built = factory.factory("/");

        assertThat(built.getUsername()).isEmpty();
        assertThat(built.getPassword()).isEqualTo("tok");
    }

    @Test
    void noCredentialsOnPlainAmqpAreRefused() {
        AmqpFactory factory =
                new AmqpFactory(cluster(new Credentials.None(), ConnectOptions.defaults(), data("amqp", "h")));

        assertThatThrownBy(() -> factory.factory("/"))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void amqpsUsesTheTlsMaterialAndVerifiesTheHost() throws Exception {
        AmqpFactory factory = new AmqpFactory(
                cluster(USER, withTls(true, List.of("TLSv1.3"), List.of()), data("amqps", "secure.example.com")));

        ConnectionFactory built = factory.factory("/");

        assertThat(built.isSSL()).isTrue();
        assertThat(built.getSocketConfigurator()).isNotNull();
        assertThat(factory.addresses()).containsExactly(new Address("secure.example.com", 5671));
    }

    @Test
    void hostNameVerificationFollowsTheMaterial() throws Exception {
        AmqpFactory factory = new AmqpFactory(
                cluster(USER, withTls(false, List.of(), List.of("TLS_AES_128_GCM_SHA256")), data("amqps", "h")));

        ConnectionFactory built = factory.factory("/");

        assertThat(built.isSSL()).isTrue();
        assertThat(built.getSocketConfigurator()).isNotNull();
    }

    @Test
    void clientCertificatesSignInWithExternal() throws Exception {
        AmqpFactory factory = new AmqpFactory(
                cluster(new Credentials.None(), withTls(true, List.of(), List.of()), data("amqps", "h")));

        assertThat(factory.factory("/").getSaslConfig()).isSameAs(DefaultSaslConfig.EXTERNAL);
    }

    @Test
    void amqpsWithoutTlsMaterialIsRefused() {
        AmqpFactory factory = new AmqpFactory(cluster(USER, ConnectOptions.defaults(), data("amqps", "h")));

        assertThatThrownBy(() -> factory.factory("/"))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e).hasMessageContaining("TlsProvider"));
    }

    @Test
    void mixedOrUnknownProtocolsAreRefused() {
        AmqpFactory mixed =
                new AmqpFactory(cluster(USER, ConnectOptions.defaults(), data("amqp", "a"), data("amqps", "b")));
        AmqpFactory unknown = new AmqpFactory(cluster(USER, ConnectOptions.defaults(), data("mqtt", "a")));

        assertThatThrownBy(() -> mixed.factory("/")).hasMessageContaining("mix");
        assertThatThrownBy(() -> unknown.factory("/")).hasMessageContaining("mqtt");
    }
}
