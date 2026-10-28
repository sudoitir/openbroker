package io.github.sudoitir.openbroker.artemis;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Volume;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * One Artemis broker of the chaos tests, behind Toxiproxy proxies on its Core and console ports.
 * Clients reach Core and Jolokia through the proxies, whose ports stay the same while the broker is
 * killed and started. A cut holds all traffic of both proxies with two toxics each, so that a client
 * sees a silent link and not a refused one, whichever port it uses.
 */
final class ChaosBroker {

    private static final int CORE = 61616;
    private static final int CONSOLE = 8161;
    private static final String SHARED = "/shared";
    private static final String ETC_OVERRIDE = "/var/lib/artemis-instance/etc-override/broker.xml";
    private static final String CUT_UP = "cut-up";
    private static final String CUT_DOWN = "cut-down";
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private final NodeId id;
    private final GenericContainer<?> container;
    private final Proxy proxy;
    private final Proxy consoleProxy;
    private final String proxyHost;
    private final int proxyPort;
    private final int consoleProxyPort;
    private final int consolePort;

    private ChaosBroker(
            NodeId id,
            GenericContainer<?> container,
            Proxy proxy,
            Proxy consoleProxy,
            String proxyHost,
            int proxyPort,
            int consoleProxyPort,
            int consolePort) {
        this.id = id;
        this.container = container;
        this.proxy = proxy;
        this.consoleProxy = consoleProxy;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.consoleProxyPort = consoleProxyPort;
        this.consolePort = consolePort;
    }

    /**
     * Starts a broker and waits for {@code ready}, a line of its log.
     *
     * @param volume a named Docker volume to keep the journal on, or empty for the container's own
     * @param haPolicy the {@code ha-policy} element, or empty for a broker that is on its own
     * @param proxy the proxy to this broker's Core port, already created on {@code alias}
     * @param consoleProxy the proxy to this broker's console port, already created on {@code alias}
     */
    static ChaosBroker start(
            NodeId id,
            String alias,
            Network network,
            Optional<String> volume,
            String haPolicy,
            String ready,
            Proxy proxy,
            Proxy consoleProxy,
            String proxyHost,
            int proxyPort,
            int consoleProxyPort) {
        int consolePort = freePort();
        String broker = read("chaos/broker.xml")
                .replace("@HA_POLICY@", haPolicy)
                .replace("@DATA@", volume.isPresent() ? SHARED : "data");
        GenericContainer<?> container = new GenericContainer<>(
                        System.getProperty("artemis.image", "apache/artemis:2.57.0"))
                .withEnv("ARTEMIS_USER", "artemis")
                .withEnv("ARTEMIS_PASSWORD", "artemis")
                .withEnv("ANONYMOUS_LOGIN", "false")
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(CONSOLE)
                .withCopyToContainer(Transferable.of(broker, 0644), ETC_OVERRIDE)
                .withCreateContainerCmdModifier(cmd -> {
                    var host = cmd.getHostConfig()
                            .withPortBindings(
                                    new PortBinding(Ports.Binding.bindPort(consolePort), new ExposedPort(CONSOLE)));
                    volume.ifPresent(name -> host.withBinds(new Bind(name, new Volume(SHARED))));
                })
                .waitingFor(Wait.forLogMessage(".*" + ready + ".*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        container.start();
        ChaosBroker started = new ChaosBroker(
                id, container, proxy, consoleProxy, proxyHost, proxyPort, consoleProxyPort, consolePort);
        // The log line says the console is up before Jolokia answers its first request.
        started.awaitConsole();
        return started;
    }

    /**
     * Makes the volume writable by the broker's user: a new volume belongs to root, and the image
     * creates its instance, and so its data directory, only when it starts.
     */
    static void prepareVolume(String volume) {
        try (GenericContainer<?> chmod = new GenericContainer<>(
                        System.getProperty("artemis.image", "apache/artemis:2.57.0"))
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withUser("root").withEntrypoint("chmod").withCmd("777", SHARED);
                    cmd.getHostConfig().withBinds(new Bind(volume, new Volume(SHARED)));
                })
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy())) {
            chmod.start();
        }
    }

    NodeId id() {
        return id;
    }

    /** The node as a connection names it: Jolokia and Core through their proxies. */
    NodeSpec spec(String host) {
        return new NodeSpec(
                id,
                List.of(
                        Endpoint.of(
                                Plane.MANAGEMENT,
                                "jolokia",
                                "http://" + host + ":" + consoleProxyPort + "/console/jolokia"),
                        Endpoint.of(Plane.DATA, "core", "tcp://" + proxyHost + ":" + proxyPort)));
    }

    boolean running() {
        return Boolean.TRUE.equals(
                container.getCurrentContainerInfo().getState().getRunning());
    }

    /** Kills the process with SIGKILL, as a crash does. */
    void kill() {
        if (running()) {
            container
                    .getDockerClient()
                    .killContainerCmd(container.getContainerId())
                    .exec();
        }
    }

    /** Starts a killed broker again, with its data, and waits until its console answers. */
    void start() {
        if (running()) {
            return;
        }
        container
                .getDockerClient()
                .startContainerCmd(container.getContainerId())
                .exec();
        awaitConsole();
    }

    private void awaitConsole() {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!consoleAnswers()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Broker " + id + " did not answer on its console");
            }
            sleep(250);
        }
    }

    void cut() {
        try {
            for (Proxy cut : List.of(proxy, consoleProxy)) {
                if (cut.toxics().getAll().isEmpty()) {
                    cut.toxics().timeout(CUT_UP, ToxicDirection.UPSTREAM, 0);
                    cut.toxics().timeout(CUT_DOWN, ToxicDirection.DOWNSTREAM, 0);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot cut " + id, e);
        }
    }

    void heal() {
        try {
            for (Proxy healed : List.of(proxy, consoleProxy)) {
                for (var toxic : healed.toxics().getAll()) {
                    toxic.remove();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot heal " + id, e);
        }
    }

    void stop() {
        for (Proxy gone : List.of(proxy, consoleProxy)) {
            try {
                gone.delete();
            } catch (IOException | RuntimeException ignored) {
                // The proxy goes with its container anyway.
            }
        }
        container.stop();
    }

    private boolean consoleAnswers() {
        try {
            String basic = Base64.getEncoder().encodeToString("artemis:artemis".getBytes(StandardCharsets.UTF_8));
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create("http://" + proxyHost + ":" + consoleProxyPort + "/console/jolokia/version"))
                    .header("Authorization", "Basic " + basic)
                    .timeout(Duration.ofSeconds(2))
                    .build();
            return HTTP.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port", e);
        }
    }

    private static String read(String resource) {
        try (var in = ChaosBroker.class.getClassLoader().getResourceAsStream(resource)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
