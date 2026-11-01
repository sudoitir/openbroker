package io.github.sudoitir.openbroker.rabbitmq;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import tools.jackson.databind.JsonNode;

/**
 * The three-node cluster of the chaos tests. Clients reach the AMQP and the management port of every node through a
 * Toxiproxy proxy of its own, so a cut holds all traffic between the client and one node while the nodes keep talking
 * to each other. The proxy ports outlive a kill and start of their node. The tests find queue leaders, and wait for
 * nodes to return, through a third proxy per node, which no toxic touches.
 */
final class RabbitMqChaosCluster implements AutoCloseable {

    private static final int NODES = 3;
    private static final int CONTROL = 8474;
    private static final int FIRST_AMQP_PORT = 8666;
    private static final int FIRST_MANAGEMENT_PORT = 8670;
    private static final int FIRST_DIRECT_PORT = 8674;
    private static final String CUT_UP = "cut-up";
    private static final String CUT_DOWN = "cut-down";
    private static final Duration RETURN = Duration.ofSeconds(180);

    private final RabbitMqTestCluster rabbit;
    private final ToxiproxyContainer toxiproxy;
    private final List<Proxy> amqp = new ArrayList<>();
    private final List<Proxy> management = new ArrayList<>();
    private final List<RabbitMq> direct = new ArrayList<>();

    RabbitMqChaosCluster(String image, String toxiproxyImage) {
        rabbit = RabbitMqTestCluster.start(image);
        Integer[] ports = new Integer[3 * NODES + 1];
        ports[0] = CONTROL;
        for (int i = 0; i < NODES; i++) {
            ports[1 + i] = FIRST_AMQP_PORT + i;
            ports[1 + NODES + i] = FIRST_MANAGEMENT_PORT + i;
            ports[1 + 2 * NODES + i] = FIRST_DIRECT_PORT + i;
        }
        toxiproxy = new ToxiproxyContainer(toxiproxyImage)
                .withNetwork(rabbit.nodes().getFirst().getNetwork())
                .withExposedPorts(ports);
        try {
            toxiproxy.start();
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            for (int i = 0; i < NODES; i++) {
                String host = RabbitMqTestCluster.host(i + 1);
                amqp.add(client.createProxy(
                        "amqp-" + (i + 1),
                        "0.0.0.0:" + (FIRST_AMQP_PORT + i),
                        host + ":" + RabbitMqTestCluster.AMQP_PORT));
                management.add(client.createProxy(
                        "management-" + (i + 1),
                        "0.0.0.0:" + (FIRST_MANAGEMENT_PORT + i),
                        host + ":" + RabbitMqTestCluster.MANAGEMENT_PORT));
                // The tests' own path never gets a toxic: they ask through it where the queues are.
                client.createProxy(
                        "direct-" + (i + 1),
                        "0.0.0.0:" + (FIRST_DIRECT_PORT + i),
                        host + ":" + RabbitMqTestCluster.MANAGEMENT_PORT);
                direct.add(RabbitMq.at(
                        RabbitMqOptions.HTTP + "://" + toxiproxy.getHost() + ":"
                                + toxiproxy.getMappedPort(FIRST_DIRECT_PORT + i),
                        RabbitMqTestCluster.ADMIN_USER,
                        RabbitMqTestCluster.ADMIN_PASSWORD));
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw new IllegalStateException("Cannot proxy the nodes", e);
        }
    }

    /** What a caller of the driver passes to reach the nodes through their proxies. */
    ClusterSpec spec() {
        List<NodeSpec> nodes = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            nodes.add(new NodeSpec(
                    NodeId.of(RabbitMqTestCluster.nodeName(i + 1)),
                    List.of(
                            Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, address(FIRST_MANAGEMENT_PORT + i)),
                            Endpoint.of(Plane.DATA, RabbitMqOptions.AMQP, address(FIRST_AMQP_PORT + i)))));
        }
        return new ClusterSpec(nodes, List.of(), Map.of());
    }

    private String address(int proxyPort) {
        String address = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(proxyPort);
        return proxyPort >= FIRST_MANAGEMENT_PORT && proxyPort < FIRST_DIRECT_PORT
                ? RabbitMqOptions.HTTP + "://" + address
                : address;
    }

    /** The proxy of node {@code index} (from 0) in front of its AMQP port. */
    Proxy amqpProxy(int index) {
        return amqp.get(index);
    }

    /** The proxy of node {@code index} (from 0) in front of its management port. */
    Proxy managementProxy(int index) {
        return management.get(index);
    }

    /** The administrator's view of the cluster through the first node that answers, untouched by any toxic. */
    private JsonNode get(String path) {
        return onAny(api -> api.get(path), path);
    }

    private <T> T onAny(Function<RabbitMq, T> call, String what) {
        RuntimeException last = null;
        for (RabbitMq api : direct) {
            try {
                return call.apply(api);
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new IllegalStateException("No node answers " + what, last);
    }

    /** The leader of the quorum queue {@code name} of the default virtual host, once it has one with a follower online. */
    NodeId leader(String name) {
        JsonNode queue = awaitQueue(name);
        return NodeId.of(queue.path("leader").asString());
    }

    /** An online follower of the quorum queue {@code name}. */
    NodeId follower(String name) {
        JsonNode queue = awaitQueue(name);
        String leader = queue.path("leader").asString();
        for (JsonNode member : queue.path("online")) {
            if (!member.asString().equals(leader)) {
                return NodeId.of(member.asString());
            }
        }
        throw new IllegalStateException("No follower online for " + name);
    }

    private JsonNode awaitQueue(String name) {
        return RabbitMq.eventually(
                Duration.ofSeconds(60),
                () -> get("/api/queues/" + RabbitMq.segment("/") + "/" + RabbitMq.segment(name)),
                queue -> !queue.path("leader").asString("").isEmpty()
                        && queue.path("online").size() > 1);
    }

    void cut(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            int index = index(node);
            for (Proxy proxy : List.of(amqp.get(index), management.get(index))) {
                try {
                    if (proxy.toxics().getAll().isEmpty()) {
                        proxy.toxics().timeout(CUT_UP, ToxicDirection.UPSTREAM, 0);
                        proxy.toxics().timeout(CUT_DOWN, ToxicDirection.DOWNSTREAM, 0);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot cut " + node, e);
                }
            }
        }
    }

    void heal(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            int index = index(node);
            for (Proxy proxy : List.of(amqp.get(index), management.get(index))) {
                try {
                    for (var toxic : proxy.toxics().getAll()) {
                        toxic.remove();
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot heal " + node, e);
                }
            }
        }
    }

    /**
     * Kills the nodes with SIGKILL, as a crash does, after the running nodes have written out their registries.
     *
     * <p>A quorum queue is registered in a DETS table that is saved every 500 ms (and its vote and term in one saved
     * every 5 s). A node killed within that time of the declaration restarts without the member ("configured member
     * was not found on this node, starting member as a new one"), with an empty log, and when the whole cluster is
     * killed that way every confirmed message is gone. The cases model the crash of a queue that has been in use, so
     * the registries are written out first.
     */
    void kill(List<NodeId> nodes) {
        rabbit.nodes().stream().filter(RabbitMqChaosCluster::running).forEach(RabbitMqChaosCluster::saveRegistries);
        for (NodeId node : nodes) {
            GenericContainer<?> container = rabbit.nodes().get(index(node));
            if (running(container)) {
                container
                        .getDockerClient()
                        .killContainerCmd(container.getContainerId())
                        .exec();
            }
        }
    }

    private static void saveRegistries(GenericContainer<?> container) {
        try {
            container.execInContainer(
                    "rabbitmqctl",
                    "eval",
                    "[dets:sync(T) || T <- dets:all(), T =:= ra_directory_reverse orelse T =:= ra_log_meta].");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot save the registries of " + container.getContainerName(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted saving the registries", e);
        }
    }

    /** Starts the nodes again, with their data, and waits until each is in service and the cluster lists it running. */
    void start(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            GenericContainer<?> container = rabbit.nodes().get(index(node));
            if (!running(container)) {
                container
                        .getDockerClient()
                        .startContainerCmd(container.getContainerId())
                        .exec();
            }
        }
        long expected =
                rabbit.nodes().stream().filter(RabbitMqChaosCluster::running).count();
        for (NodeId node : nodes) {
            RabbitMq api = direct.get(index(node));
            RabbitMq.eventually(RETURN, () -> api.exists("/api/health/checks/virtual-hosts"), inService -> inService);
        }
        RabbitMq.eventually(RETURN, () -> get("/api/nodes"), listed -> running(listed) == expected);
    }

    private static long running(JsonNode nodes) {
        long running = 0;
        for (JsonNode node : nodes) {
            if (node.path("running").asBoolean(false)) {
                running++;
            }
        }
        return running;
    }

    /** Declares a quorum queue on the default virtual host, replicated on every node. */
    void declareQuorumQueue(String name) {
        RabbitMq.eventually(
                Duration.ofSeconds(60),
                () -> onAny(
                        api -> {
                            api.declareQueue("/", name, "quorum");
                            return true;
                        },
                        name),
                declared -> declared);
    }

    /** Publishes {@code payload} to the queue {@code name} through the default exchange. */
    void publish(String name, String payload) {
        RabbitMq.eventually(
                Duration.ofSeconds(60),
                () -> onAny(api -> api.publish("/", "", name, payload), name),
                routed -> routed);
    }

    /** The number of messages the queue holds, ready or not. */
    long messages(String name) {
        return get("/api/queues/" + RabbitMq.segment("/") + "/" + RabbitMq.segment(name))
                .path("messages")
                .asLong();
    }

    /** Deletes the queue; the queue may be gone already. */
    void deleteQueue(String name) {
        try {
            onAny(
                    api -> {
                        api.deleteQueue("/", name);
                        return true;
                    },
                    name);
        } catch (RuntimeException e) {
            // Best effort: the cluster is torn down at the end.
        }
    }

    /** Every node. */
    static List<NodeId> all() {
        return IntStream.rangeClosed(1, NODES)
                .mapToObj(id -> NodeId.of(RabbitMqTestCluster.nodeName(id)))
                .toList();
    }

    private static int index(NodeId node) {
        String value = node.value();
        return Integer.parseInt(value.substring(value.lastIndexOf('-') + 1)) - 1;
    }

    private static boolean running(GenericContainer<?> container) {
        return Boolean.TRUE.equals(
                container.getCurrentContainerInfo().getState().getRunning());
    }

    @Override
    public void close() {
        toxiproxy.stop();
        rabbit.close();
    }
}
