package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * The Artemis broker the messaging integration tests run against: the image's defaults plus an
 * account that may use {@code tck.*} but not {@code tck.denied.*}, and a TLS acceptor that requires
 * a client certificate. The configuration is in {@code src/test/resources/broker}; the throwaway
 * certificates are made by {@code src/test/resources/tls/regenerate.sh}.
 */
final class TestBroker {

    static final String USER = "artemis";
    static final String PASSWORD = "artemis";
    static final String RESTRICTED_USER = "restricted";
    static final String RESTRICTED_PASSWORD = "restricted";

    private static final String STORE_PASSWORD = "openbroker";
    private static final String TLS_DIRECTORY = "/etc/openbroker-tls/";
    private static final String ETC_OVERRIDE = "/var/lib/artemis-instance/etc-override/";
    private static final int CORE = 61616;
    private static final int MUTUAL_TLS = 61617;
    private static final int CONSOLE = 8161;

    private TestBroker() {}

    /** A broker of the image the build names ({@code artemis.image}), not yet started. */
    static GenericContainer<?> container() {
        return new GenericContainer<>(System.getProperty("artemis.image", "apache/artemis:2.57.0"))
                .withEnv("ARTEMIS_USER", USER)
                .withEnv("ARTEMIS_PASSWORD", PASSWORD)
                .withEnv("ANONYMOUS_LOGIN", "false")
                .withExposedPorts(CORE, MUTUAL_TLS, CONSOLE)
                .withCopyFileToContainer(readable("tls/broker.p12"), TLS_DIRECTORY + "broker.p12")
                .withCopyFileToContainer(readable("tls/truststore.p12"), TLS_DIRECTORY + "truststore.p12")
                .withCopyFileToContainer(readable("broker/broker.xml"), ETC_OVERRIDE + "broker.xml")
                .withCopyFileToContainer(readable("broker/login.config"), ETC_OVERRIDE + "login.config")
                .withCopyFileToContainer(
                        readable("broker/artemis-users.properties"), ETC_OVERRIDE + "artemis-users.properties")
                .withCopyFileToContainer(
                        readable("broker/artemis-roles.properties"), ETC_OVERRIDE + "artemis-roles.properties")
                .withCopyFileToContainer(
                        readable("broker/cert-users.properties"), ETC_OVERRIDE + "cert-users.properties")
                .withCopyFileToContainer(
                        readable("broker/cert-roles.properties"), ETC_OVERRIDE + "cert-roles.properties")
                .waitingFor(Wait.forLogMessage(".*Artemis Console available.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /** A request for the broker, as {@code user}, on the plain acceptor. */
    static ConnectRequest request(GenericContainer<?> broker, String user, String password) {
        return new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(node(broker, CORE))),
                CredentialSource.of(Credentials.of(user, password)),
                ConnectOptions.defaults());
    }

    /**
     * A request through the TLS acceptor, which asks for a client certificate and logs the client in
     * by it: the data plane sends no password, and only the console, which has no certificate login,
     * gets one.
     */
    static ConnectRequest mutualTls(GenericContainer<?> broker) {
        return mutualTls(broker, TlsProvider.of(clientMaterial(broker, "openbroker-artemis-tck-client")));
    }

    /** {@link #mutualTls(GenericContainer)} with TLS material from {@code tls}, which may change. */
    static ConnectRequest mutualTls(GenericContainer<?> broker, TlsProvider tls) {
        CredentialSource credentials =
                plane -> plane == Plane.DATA ? Credentials.none() : Credentials.of(USER, PASSWORD);
        return new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(node(broker, MUTUAL_TLS))),
                credentials,
                ConnectOptions.defaults().withTls(tls));
    }

    /** The test client certificate, under the given id: material with another id is a rotation. */
    static TlsMaterial clientMaterial(GenericContainer<?> broker, String id) {
        return new TlsMaterial(id, clientContext(), isLoopback(broker.getHost()), List.of(), List.of());
    }

    static NodeSpec node(GenericContainer<?> broker, int corePort) {
        String host = broker.getHost();
        return new NodeSpec(
                NodeId.of("primary"),
                List.of(
                        Endpoint.of(
                                Plane.MANAGEMENT,
                                "jolokia",
                                "http://" + host + ":" + broker.getMappedPort(CONSOLE) + "/console/jolokia"),
                        Endpoint.of(Plane.DATA, "core", "tcp://" + host + ":" + broker.getMappedPort(corePort))));
    }

    private static MountableFile readable(String resource) {
        return MountableFile.forClasspathResource(resource, 0644);
    }

    /** The throwaway certificate is for localhost; against any other host the name cannot match. */
    private static boolean isLoopback(String host) {
        return host.equals("localhost") || host.equals("127.0.0.1");
    }

    private static SSLContext clientContext() {
        try {
            KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(store("tls/client.p12"), STORE_PASSWORD.toCharArray());
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store("tls/truststore.p12"));
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers.getKeyManagers(), trust.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException("The test TLS stores cannot be read; run tls/regenerate.sh", e);
        }
    }

    private static KeyStore store(String resource) throws GeneralSecurityException, IOException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = TestBroker.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("No test resource " + resource);
            }
            store.load(in, STORE_PASSWORD.toCharArray());
        }
        return store;
    }
}
