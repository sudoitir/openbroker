package io.github.sudoitir.openbroker.kafka;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * The three-broker cluster of the chaos tests. Clients reach every broker through its own Toxiproxy
 * proxy, and the broker advertises that proxy to them, so a cut holds all traffic between the client
 * and one broker while the brokers keep talking to each other. The proxy port outlives a kill and
 * start of its broker. The tests find leaders and followers, and wait for brokers to return, through a
 * second listener behind proxies of its own, which no toxic touches.
 */
final class KafkaChaosCluster implements AutoCloseable {

    static final String USER = KafkaSaslBroker.USER;
    static final String PASSWORD = KafkaSaslBroker.PASSWORD;

    private static final int BROKERS = 3;
    private static final int CLIENT = 9092;
    private static final int INTERNAL = 9093;
    private static final int CONTROLLER = 9094;
    private static final int DIRECT = 9095;
    private static final int CONTROL = 8474;
    private static final int FIRST_PROXY_PORT = 8666;
    private static final int FIRST_DIRECT_PROXY_PORT = 8670;
    private static final String CUT_UP = "cut-up";
    private static final String CUT_DOWN = "cut-down";
    private static final String OFFSETS_TOPIC = "__consumer_offsets";
    private static final Duration RETURN = Duration.ofSeconds(120);

    private final Network network = Network.newNetwork();
    private final ToxiproxyContainer toxiproxy;
    private final List<GenericContainer<?>> brokers = new ArrayList<>();
    private final List<Proxy> proxies = new ArrayList<>();

    KafkaChaosCluster(String image, String toxiproxyImage) {
        Integer[] ports = new Integer[2 * BROKERS + 1];
        ports[0] = CONTROL;
        for (int i = 0; i < BROKERS; i++) {
            ports[i + 1] = FIRST_PROXY_PORT + i;
            ports[BROKERS + i + 1] = FIRST_DIRECT_PROXY_PORT + i;
        }
        toxiproxy = new ToxiproxyContainer(toxiproxyImage).withNetwork(network).withExposedPorts(ports);
        toxiproxy.start();
        try {
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            for (int i = 0; i < BROKERS; i++) {
                proxies.add(client.createProxy(
                        "kafka-" + (i + 1),
                        "0.0.0.0:" + (FIRST_PROXY_PORT + i),
                        KafkaTestCluster.host(i + 1) + ":" + CLIENT));
                // The tests' own path never gets a toxic: they ask through it where the brokers are.
                client.createProxy(
                        "kafka-" + (i + 1) + "-direct",
                        "0.0.0.0:" + (FIRST_DIRECT_PROXY_PORT + i),
                        KafkaTestCluster.host(i + 1) + ":" + DIRECT);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot proxy the brokers", e);
        }
        for (int i = 0; i < BROKERS; i++) {
            brokers.add(broker(image, i));
        }
        try {
            Startables.deepStart(brokers).join();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        // The first group to ask for its coordinator makes the offsets topic, which takes longer than a test's call
        // timeout.
        await("the offsets topic", RETURN, admin -> {
            admin.listConsumerGroupOffsets("probe").all().get(10, TimeUnit.SECONDS);
            return Optional.of(true);
        });
    }

    private GenericContainer<?> broker(String image, int index) {
        int id = index + 1;
        String host = KafkaTestCluster.host(id);
        Map<String, String> env = new HashMap<>(KafkaTestCluster.environment(id, KafkaTestCluster.voters()));
        env.put("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw");
        env.put("KAFKA_PROCESS_ROLES", "broker,controller");
        env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
        env.put(
                "KAFKA_LISTENERS",
                "PLAINTEXT://:" + CLIENT + ",BROKER://:" + INTERNAL + ",CONTROLLER://:" + CONTROLLER + ",DIRECT://:"
                        + DIRECT);
        // Clients log in, so a reconnect asks the credential source again; the brokers and the tests' own
        // admin client use listeners of their own.
        env.put(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,DIRECT:PLAINTEXT");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put(
                "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required user_" + USER + "=\"" + PASSWORD
                        + "\";");
        env.put(
                "KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(FIRST_PROXY_PORT + index)
                        + ",BROKER://" + host + ":" + INTERNAL + ",DIRECT://" + toxiproxy.getHost() + ":"
                        + toxiproxy.getMappedPort(FIRST_DIRECT_PROXY_PORT + index));
        return new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(host)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host))
                .withEnv(env)
                .waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    /** What a caller of the driver passes to reach the brokers through their proxies. */
    ClusterSpec spec() {
        List<NodeSpec> nodes = new ArrayList<>();
        for (int i = 0; i < BROKERS; i++) {
            nodes.add(new NodeSpec(
                    NodeId.of(String.valueOf(i + 1)),
                    List.of(Endpoint.of(
                            Plane.DATA,
                            KafkaOptions.PROTOCOL,
                            toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(FIRST_PROXY_PORT + i)))));
        }
        return new ClusterSpec(
                nodes,
                List.of(),
                Map.of(
                        KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_PLAINTEXT,
                        KafkaOptions.SASL_MECHANISM, KafkaOptions.PLAIN));
    }

    /** The leader of the first partition of {@code topic}, once it has one with a follower in sync. */
    NodeId leader(String topic) {
        return NodeId.of(Integer.toString(awaitPartition(topic).leader().id()));
    }

    /** An in-sync follower of the first partition of {@code topic}. */
    NodeId follower(String topic) {
        TopicPartitionInfo partition = awaitPartition(topic);
        return partition.isr().stream()
                .filter(node -> node.id() != partition.leader().id())
                .findFirst()
                .map(node -> NodeId.of(Integer.toString(node.id())))
                .orElseThrow();
    }

    private TopicPartitionInfo awaitPartition(String topic) {
        return await("a leader and a follower in sync for " + topic, Duration.ofSeconds(60), admin -> {
            TopicDescription description = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic);
            TopicPartitionInfo partition = description.partitions().getFirst();
            return partition.leader() != null && partition.isr().size() > 1 ? Optional.of(partition) : Optional.empty();
        });
    }

    void cut(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            Proxy proxy = proxies.get(index(node));
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

    void heal(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            try {
                for (var toxic : proxies.get(index(node)).toxics().getAll()) {
                    toxic.remove();
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot heal " + node, e);
            }
        }
    }

    /** Kills the brokers with SIGKILL, as a crash does. */
    void kill(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            GenericContainer<?> broker = brokers.get(index(node));
            if (running(broker)) {
                broker.getDockerClient()
                        .killContainerCmd(broker.getContainerId())
                        .exec();
            }
        }
    }

    /** Starts the brokers again, with their data, and waits until the cluster sees all of them. */
    void start(List<NodeId> nodes) {
        for (NodeId node : nodes) {
            GenericContainer<?> broker = brokers.get(index(node));
            if (!running(broker)) {
                broker.getDockerClient()
                        .startContainerCmd(broker.getContainerId())
                        .exec();
            }
        }
        Set<Integer> expected = nodes.stream().map(n -> index(n) + 1).collect(Collectors.toSet());
        await("the brokers " + expected, RETURN, admin -> {
            Set<Integer> seen = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).stream()
                    .map(Node::id)
                    .collect(Collectors.toSet());
            return seen.containsAll(expected) ? Optional.of(seen) : Optional.empty();
        });
        long running = brokers.stream().filter(KafkaChaosCluster::running).count();
        await("the group coordinator", RETURN, admin -> {
            var offsets = admin.describeTopics(List.of(OFFSETS_TOPIC))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(OFFSETS_TOPIC);
            boolean settled = offsets.partitions().stream()
                    .allMatch(p -> p.leader() != null && p.isr().size() >= running);
            if (!settled) {
                return Optional.empty();
            }
            // Every coordinator has loaded its groups once it answers for one of its own.
            Map<String, ListConsumerGroupOffsetsSpec> probes = new HashMap<>();
            coordinatorProbes(offsets.partitions().size())
                    .forEach(group -> probes.put(group, new ListConsumerGroupOffsetsSpec()));
            admin.listConsumerGroupOffsets(probes).all().get(10, TimeUnit.SECONDS);
            return Optional.of(true);
        });
        // Clients list groups on every broker, so each must answer on the listener they use.
        await("every broker on the client listener", RETURN, viaProxies(), admin -> {
            admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            return Optional.of(true);
        });
    }

    /** An Admin client's settings for the way clients reach the brokers: through the proxies, logged in. */
    private Map<String, Object> viaProxies() {
        String bootstrap = IntStream.range(0, BROKERS)
                .mapToObj(i -> toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(FIRST_PROXY_PORT + i))
                .collect(Collectors.joining(","));
        return Map.of(
                "bootstrap.servers",
                bootstrap,
                "security.protocol",
                "SASL_PLAINTEXT",
                "sasl.mechanism",
                "PLAIN",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USER + "\" password=\""
                        + PASSWORD + "\";");
    }

    /** Group ids that cover every partition of the offsets topic, which is how a group finds its coordinator. */
    private static List<String> coordinatorProbes(int partitions) {
        Map<Integer, String> byPartition = new HashMap<>();
        for (int i = 0; byPartition.size() < partitions; i++) {
            String group = "probe-" + i;
            byPartition.putIfAbsent(Math.abs(group.hashCode() % partitions), group);
        }
        return List.copyOf(byPartition.values());
    }

    /** Every broker. */
    static List<NodeId> all() {
        return IntStream.rangeClosed(1, BROKERS)
                .mapToObj(id -> NodeId.of(String.valueOf(id)))
                .toList();
    }

    private static int index(NodeId node) {
        return Integer.parseInt(node.value()) - 1;
    }

    private static boolean running(GenericContainer<?> broker) {
        return Boolean.TRUE.equals(broker.getCurrentContainerInfo().getState().getRunning());
    }

    @FunctionalInterface
    private interface Probe<T> {
        Optional<T> read(Admin admin) throws ExecutionException, InterruptedException, TimeoutException;
    }

    /** Asks the brokers directly until the probe has an answer. */
    private <T> T await(String what, Duration within, Probe<T> probe) {
        String bootstrap = IntStream.range(0, BROKERS)
                .mapToObj(i -> toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(FIRST_DIRECT_PROXY_PORT + i))
                .collect(Collectors.joining(","));
        return await(what, within, Map.of("bootstrap.servers", bootstrap), probe);
    }

    /** Asks until the probe has an answer, with an Admin client of {@code base} settings. */
    private <T> T await(String what, Duration within, Map<String, Object> base, Probe<T> probe) {
        long deadline = System.nanoTime() + within.toNanos();
        Exception last = null;
        while (true) {
            Map<String, Object> config = new HashMap<>(base);
            config.put("request.timeout.ms", "5000");
            config.put("default.api.timeout.ms", "10000");
            try (Admin admin = Admin.create(config)) {
                Optional<T> answer = probe.read(admin);
                if (answer.isPresent()) {
                    return answer.get();
                }
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                // The brokers are not ready yet: ask again.
                last = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for " + what, e);
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Timed out waiting for " + what + "\n" + logs(), last);
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for " + what, e);
            }
        }
    }

    /** The end of each broker's log, for a failure that only shows on a machine we cannot see. */
    private String logs() {
        StringBuilder out = new StringBuilder();
        for (GenericContainer<?> broker : brokers) {
            String[] lines = broker.getLogs().split("\n");
            out.append("--- ").append(broker.getContainerName()).append('\n');
            for (int i = Math.max(0, lines.length - 25); i < lines.length; i++) {
                out.append(lines[i]).append('\n');
            }
        }
        return out.toString();
    }

    @Override
    public void close() {
        brokers.forEach(GenericContainer::stop);
        toxiproxy.stop();
        network.close();
    }
}
