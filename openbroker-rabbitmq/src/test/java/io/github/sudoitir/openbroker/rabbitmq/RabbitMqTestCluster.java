package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

/**
 * A three-node RabbitMQ cluster in Docker, the one every RabbitMQ integration test runs against, and the variants a
 * test may need beside it: one node with a chosen set of plugins (without the event exchange, for the watch that
 * polls), and one node that also listens over TLS. The nodes are {@code rabbit@rabbit-1} to {@code rabbit@rabbit-3}
 * on one network, share an Erlang cookie and form the cluster by classic peer discovery, which behaves alike on
 * 3.13 and on 4.x. The image is the {@code rabbitmq.image} system property, by default the newest the driver is
 * tested on.
 */
public final class RabbitMqTestCluster implements AutoCloseable {

    /** The system property that names the image, such as {@code rabbitmq:3.13.7-management}. */
    public static final String IMAGE_PROPERTY = "rabbitmq.image";

    /** The image the tests run on when the build names none. */
    public static final String DEFAULT_IMAGE = "rabbitmq:4.4.0-management";

    /** The administrator every node has, as {@code rabbitmq.conf} makes it. */
    public static final String ADMIN_USER = "admin";

    /** The administrator's password. */
    public static final String ADMIN_PASSWORD = "admin-secret";

    /** The management port inside the container. */
    public static final int MANAGEMENT_PORT = 15672;

    /** The management port over TLS inside the container, on the TLS variant. */
    public static final int MANAGEMENT_TLS_PORT = 15671;

    /** The AMQP 0-9-1 port inside the container. */
    public static final int AMQP_PORT = 5672;

    /** The AMQP 0-9-1 port over TLS inside the container, on the TLS variant. */
    public static final int AMQP_TLS_PORT = 5671;

    /** The plugins a full node runs (the contents of the {@code enabled_plugins} test resource). */
    public static final String ALL_PLUGINS = resource("enabled_plugins").trim();

    /** The plugin that publishes events on {@code amq.rabbitmq.event}. */
    public static final String EVENT_EXCHANGE_PLUGIN = "rabbitmq_event_exchange";

    private static final int NODES = 3;
    private static final String COOKIE = "OPENBROKERTESTCOOKIE";
    private static final String TLS_DIRECTORY = "/etc/rabbitmq/tls";
    private static final Duration START_TIMEOUT = Duration.ofMinutes(4);
    private static final Duration FORMED_TIMEOUT = Duration.ofMinutes(2);
    private static final int FILE_MODE = 0644;

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> nodes = new ArrayList<>();
    private final TestPki pki;

    private RabbitMqTestCluster(String image, int count, String plugins, TestPki pki) {
        this.pki = pki;
        String base = resource("rabbitmq.conf");
        for (int id = 1; id <= count; id++) {
            String host = host(id);
            GenericContainer<?> node = new GenericContainer<>(image)
                    .withNetwork(network)
                    .withNetworkAliases(host)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host))
                    .withEnv("RABBITMQ_NODENAME", nodeName(id))
                    .withEnv("RABBITMQ_ERLANG_COOKIE", COOKIE)
                    .withEnv("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "-setcookie " + COOKIE)
                    .withCopyToContainer(
                            Transferable.of(config(base, count, pki), FILE_MODE), "/etc/rabbitmq/rabbitmq.conf")
                    .withCopyToContainer(Transferable.of(plugins + "\n", FILE_MODE), "/etc/rabbitmq/enabled_plugins")
                    .withExposedPorts(exposedPorts(pki))
                    .waitingFor(Wait.forHttp("/api/health/checks/is-in-service")
                            .forPort(MANAGEMENT_PORT)
                            .withBasicCredentials(ADMIN_USER, ADMIN_PASSWORD)
                            .forStatusCode(200)
                            .withStartupTimeout(START_TIMEOUT));
            if (pki != null) {
                copyTls(node, pki);
            }
            nodes.add(node);
        }
    }

    private static Integer[] exposedPorts(TestPki pki) {
        return pki == null
                ? new Integer[] {AMQP_PORT, MANAGEMENT_PORT}
                : new Integer[] {AMQP_PORT, MANAGEMENT_PORT, AMQP_TLS_PORT, MANAGEMENT_TLS_PORT};
    }

    private static void copyTls(GenericContainer<?> node, TestPki pki) {
        for (String file : List.of("ca.pem", "broker.pem", "broker.key")) {
            node.withCopyFileToContainer(
                    MountableFile.forHostPath(pki.file(file), FILE_MODE), TLS_DIRECTORY + "/" + file);
        }
    }

    /** {@code rabbitmq.conf} for a node: the shared settings, then the cluster formation or the TLS listeners. */
    static String config(String base, int count, TestPki pki) {
        StringBuilder conf = new StringBuilder(base).append('\n');
        if (count > 1) {
            conf.append("cluster_formation.peer_discovery_backend = classic_config\n");
            for (int id = 1; id <= count; id++) {
                conf.append("cluster_formation.classic_config.nodes.")
                        .append(id)
                        .append(" = ")
                        .append(nodeName(id))
                        .append('\n');
            }
        }
        if (pki != null) {
            conf.append("""
                    listeners.ssl.default = 5671
                    ssl_options.cacertfile = %1$s/ca.pem
                    ssl_options.certfile = %1$s/broker.pem
                    ssl_options.keyfile = %1$s/broker.key
                    ssl_options.verify = verify_peer
                    ssl_options.fail_if_no_peer_cert = false
                    management.ssl.port = 15671
                    management.ssl.cacertfile = %1$s/ca.pem
                    management.ssl.certfile = %1$s/broker.pem
                    management.ssl.keyfile = %1$s/broker.key
                    auth_mechanisms.1 = PLAIN
                    auth_mechanisms.2 = AMQPLAIN
                    auth_mechanisms.3 = EXTERNAL
                    ssl_cert_login_from = common_name
                    """.formatted(TLS_DIRECTORY));
        }
        return conf.toString();
    }

    private static String resource(String name) {
        try (InputStream in = RabbitMqTestCluster.class.getResourceAsStream("/rabbitmq/" + name)) {
            if (in == null) {
                throw new IllegalStateException("Missing test resource rabbitmq/" + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The image the tests run on. */
    public static String image() {
        return System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
    }

    private static RabbitMqTestCluster shared;

    /**
     * The cluster every test class of this JVM shares: forming three nodes takes longer than most tests. It is
     * stopped when the JVM ends. A test that changes the cluster itself starts its own.
     */
    public static synchronized RabbitMqTestCluster shared() {
        if (shared == null) {
            shared = start();
            Runtime.getRuntime().addShutdownHook(new Thread(shared::close));
        }
        return shared;
    }

    /** Starts the cluster on the image the build names. */
    public static RabbitMqTestCluster start() {
        return start(image());
    }

    /** Starts the three nodes together and returns once the cluster lists all three as running. */
    public static RabbitMqTestCluster start(String image) {
        return launch(new RabbitMqTestCluster(image, NODES, ALL_PLUGINS, null), NODES);
    }

    /**
     * Starts one node with the management plugin and {@code plugins} (such as {@link #EVENT_EXCHANGE_PLUGIN}), and
     * no other. A test of the polling fallback starts it with none, so the event exchange does not exist.
     */
    public static RabbitMqTestCluster single(String... plugins) {
        return launch(new RabbitMqTestCluster(image(), 1, pluginList(plugins), null), 1);
    }

    /** Starts one node with every plugin and TLS listeners (5671 and 15671) with the certificates of {@code pki}. */
    public static RabbitMqTestCluster singleTls(TestPki pki) {
        return launch(new RabbitMqTestCluster(image(), 1, ALL_PLUGINS, pki), 1);
    }

    private static String pluginList(String... plugins) {
        return "[rabbitmq_management" + (plugins.length == 0 ? "" : "," + String.join(",", plugins)) + "].";
    }

    private static RabbitMqTestCluster launch(RabbitMqTestCluster cluster, int expected) {
        try {
            Startables.deepStart(cluster.nodes).join();
            RabbitMq.eventually(FORMED_TIMEOUT, () -> RabbitMq.of(cluster, 0).runningNodes(), n -> n == expected);
        } catch (RuntimeException e) {
            cluster.close();
            throw e;
        }
        return cluster;
    }

    /** The network alias and host name of node {@code id} (from 1). */
    public static String host(int id) {
        return "rabbit-" + id;
    }

    /** The Erlang node name of node {@code id} (from 1). */
    public static String nodeName(int id) {
        return "rabbit@" + host(id);
    }

    /** The nodes, in order. */
    public List<GenericContainer<?>> nodes() {
        return List.copyOf(nodes);
    }

    /** The administrator's credentials, as the driver is given them. */
    public Credentials credentials() {
        return Credentials.usernamePassword(ADMIN_USER, ADMIN_PASSWORD);
    }

    /** The certificates of the TLS variant, or {@code null} on the others. */
    public TestPki pki() {
        return pki;
    }

    /** The host:port a client outside Docker reaches node {@code index} (from 0) at, on the container port. */
    private String address(int index, int port) {
        GenericContainer<?> node = nodes.get(index);
        return node.getHost() + ":" + node.getMappedPort(port);
    }

    /** The management API of node {@code index} (from 0) in the clear, such as {@code http://localhost:32768}. */
    public String managementUrl(int index) {
        return RabbitMqOptions.HTTP + "://" + address(index, MANAGEMENT_PORT);
    }

    /** The management API of node {@code index} (from 0) over TLS; the TLS variant only. */
    public String managementTlsUrl(int index) {
        requireTls();
        return RabbitMqOptions.HTTPS + "://" + address(index, MANAGEMENT_TLS_PORT);
    }

    /** The AMQP address of node {@code index} (from 0) in the clear, such as {@code amqp://localhost:32769}. */
    public String amqpUrl(int index) {
        return RabbitMqOptions.AMQP + "://" + address(index, AMQP_PORT);
    }

    /** The AMQP address of node {@code index} (from 0) over TLS; the TLS variant only. */
    public String amqpTlsUrl(int index) {
        requireTls();
        return RabbitMqOptions.AMQPS + "://" + address(index, AMQP_TLS_PORT);
    }

    /** The management addresses of every node, in the clear. */
    public List<String> managementUrls() {
        return IntStream.range(0, nodes.size()).mapToObj(this::managementUrl).toList();
    }

    /** The AMQP addresses of every node, in the clear. */
    public List<String> amqpUrls() {
        return IntStream.range(0, nodes.size()).mapToObj(this::amqpUrl).toList();
    }

    private void requireTls() {
        if (pki == null) {
            throw new IllegalStateException("This cluster has no TLS listeners; start it with singleTls");
        }
    }

    /** What a caller of the driver passes to reach this cluster, in the clear. */
    public ClusterSpec spec() {
        return spec(false);
    }

    /** What a caller of the driver passes to reach this cluster over TLS; the TLS variant only. */
    public ClusterSpec tlsSpec() {
        return spec(true);
    }

    private ClusterSpec spec(boolean tls) {
        if (tls) {
            requireTls();
        }
        List<NodeSpec> specs = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            Endpoint management = Endpoint.of(
                    Plane.MANAGEMENT,
                    tls ? RabbitMqOptions.HTTPS : RabbitMqOptions.HTTP,
                    tls ? managementTlsUrl(i) : managementUrl(i));
            Endpoint data = Endpoint.of(
                    Plane.DATA, tls ? RabbitMqOptions.AMQPS : RabbitMqOptions.AMQP, tls ? amqpTlsUrl(i) : amqpUrl(i));
            specs.add(new NodeSpec(NodeId.of(nodeName(i + 1)), List.of(management, data)));
        }
        return new ClusterSpec(specs, List.of(), Map.of());
    }

    /** A spec with only the management endpoints, for the connection that has no DATA plane. */
    public ClusterSpec managementOnlySpec() {
        List<NodeSpec> specs = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            specs.add(new NodeSpec(
                    NodeId.of(nodeName(i + 1)),
                    List.of(Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, managementUrl(i)))));
        }
        return new ClusterSpec(specs, List.of(), Map.of());
    }

    /** Runs {@code rabbitmqctl} with {@code args} inside node {@code index} (from 0) and fails unless it succeeds. */
    public String rabbitmqctl(int index, String... args) {
        String[] command = new String[args.length + 1];
        command[0] = "rabbitmqctl";
        System.arraycopy(args, 0, command, 1, args.length);
        try {
            ExecResult result = nodes.get(index).execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("rabbitmqctl " + String.join(" ", args) + " failed: " + result);
            }
            return result.getStdout();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        nodes.forEach(GenericContainer::stop);
        network.close();
        if (pki != null) {
            deleteQuietly(pki);
        }
    }

    private static void deleteQuietly(TestPki pki) {
        try (var files = Files.list(pki.file("."))) {
            files.forEach(path -> path.toFile().delete());
        } catch (IOException e) {
            // The certificates are in the temporary directory, which the system clears.
        }
    }
}
