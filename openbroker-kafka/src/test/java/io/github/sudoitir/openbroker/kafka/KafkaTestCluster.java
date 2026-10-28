package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

/**
 * A three-broker cluster in Docker, the one every Kafka integration test runs against. On Apache
 * Kafka's images it runs in KRaft mode and each broker is also a controller (combined roles); on a
 * Confluent Platform image ({@code confluentinc/cp-kafka}) it runs in ZooKeeper mode, with one
 * ZooKeeper server. Topics have three replicas and need two in sync, and an out-of-sync replica can
 * never become leader, so a test that stops a broker sees what a production cluster would do. The
 * image is the {@code kafka.image} system property, by default the newest the driver is tested on.
 */
public final class KafkaTestCluster implements AutoCloseable {

    /** The system property that names the image, such as {@code apache/kafka:4.0.2}. */
    public static final String IMAGE_PROPERTY = "kafka.image";

    public static final String DEFAULT_IMAGE = "apache/kafka:4.4.0";

    /** The prefix of the images that run in ZooKeeper mode. */
    private static final String ZOOKEEPER_IMAGE = "confluentinc/cp-kafka:";

    private static final int BROKERS = 3;
    private static final int CONTROLLER_PORT = 9094;

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> brokers = new ArrayList<>();
    private GenericContainer<?> zookeeper;

    private KafkaTestCluster(String image) {
        if (zookeeperMode(image)) {
            zookeeper = new GenericContainer<>(image.replace("cp-kafka", "cp-zookeeper"))
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
                    .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                    .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                    .waitingFor(Wait.forLogMessage(".*binding to port.*", 1));
            for (int id = 1; id <= BROKERS; id++) {
                String host = host(id);
                brokers.add(new ZooKeeperKafkaBroker(image, host)
                        .withSettings(zookeeperEnvironment(id))
                        .withNetwork(network)
                        .withNetworkAliases(host)
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host)));
            }
            return;
        }
        String voters = voters();
        for (int id = 1; id <= BROKERS; id++) {
            String host = host(id);
            brokers.add(new KafkaContainer(image)
                    .withNetwork(network)
                    .withNetworkAliases(host)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host))
                    .withEnv(environment(id, voters)));
        }
    }

    /** Whether {@code image} runs a ZooKeeper-mode cluster. */
    static boolean zookeeperMode(String image) {
        return image.startsWith(ZOOKEEPER_IMAGE);
    }

    /** The image of a KRaft broker: the build's, or the default where the build runs ZooKeeper mode. */
    public static String kraftImage() {
        return zookeeperMode(image()) ? DEFAULT_IMAGE : image();
    }

    /** The image the tests run on. */
    public static String image() {
        return System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
    }

    private static KafkaTestCluster shared;

    /**
     * The cluster every test class of this JVM shares: starting three brokers takes longer than most
     * tests. It is stopped when the JVM ends. A test that changes the cluster itself starts its own.
     */
    public static synchronized KafkaTestCluster shared() {
        if (shared == null) {
            shared = start();
            Runtime.getRuntime().addShutdownHook(new Thread(shared::close));
        }
        return shared;
    }

    /** Starts the cluster on the image the build names. */
    public static KafkaTestCluster start() {
        return start(image());
    }

    /** Starts the three brokers together, because a controller quorum needs a majority to elect. */
    public static KafkaTestCluster start(String image) {
        KafkaTestCluster cluster = new KafkaTestCluster(image);
        try {
            if (cluster.zookeeper != null) {
                cluster.zookeeper.start();
            }
            Startables.deepStart(cluster.brokers).join();
        } catch (RuntimeException e) {
            cluster.close();
            throw e;
        }
        return cluster;
    }

    static String voters() {
        return IntStream.rangeClosed(1, BROKERS)
                .mapToObj(id -> id + "@" + host(id) + ":" + CONTROLLER_PORT)
                .collect(Collectors.joining(","));
    }

    /** The network alias of broker {@code id}. */
    static String host(int id) {
        return "kafka-" + id;
    }

    /** The settings of broker {@code id}, which a fixture may extend with its own listeners. */
    static Map<String, String> environment(int id, String voters) {
        return Map.ofEntries(
                Map.entry("KAFKA_NODE_ID", String.valueOf(id)),
                Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS", voters),
                Map.entry("KAFKA_DEFAULT_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_MIN_INSYNC_REPLICAS", "2"),
                Map.entry("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false"),
                Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
                // Groups rebalance on their members' heartbeats: at the default five seconds a join takes
                // a quarter of a minute, and a member cut off is noticed after forty-five.
                Map.entry("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS", "250"),
                Map.entry("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS", "500"),
                Map.entry("KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS", "1000"),
                Map.entry("KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS", "6000"),
                // Share groups heartbeat every five seconds and hold a record for at least fifteen.
                Map.entry("KAFKA_GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS", "250"),
                Map.entry("KAFKA_GROUP_SHARE_HEARTBEAT_INTERVAL_MS", "500"),
                Map.entry("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS", "1000"),
                Map.entry("KAFKA_GROUP_SHARE_MIN_DELIVERY_COUNT_LIMIT", "2"),
                // Authorization is on, as on a production cluster; the plaintext listener's principal is a super user.
                Map.entry("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
                Map.entry("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true"),
                Map.entry("KAFKA_SUPER_USERS", "User:ANONYMOUS"),
                // Three brokers on one CI runner: small heaps keep them from starving each other.
                Map.entry("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m"));
    }

    /** The settings of a ZooKeeper-mode broker: the same durability as the KRaft brokers, with ZooKeeper's authorizer. */
    static Map<String, String> zookeeperEnvironment(int id) {
        return Map.ofEntries(
                Map.entry("KAFKA_BROKER_ID", String.valueOf(id)),
                Map.entry("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181"),
                Map.entry("KAFKA_DEFAULT_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_MIN_INSYNC_REPLICAS", "2"),
                Map.entry("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false"),
                Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", String.valueOf(BROKERS)),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
                Map.entry("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                Map.entry("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer"),
                Map.entry("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true"),
                Map.entry("KAFKA_SUPER_USERS", "User:ANONYMOUS"),
                Map.entry("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m"));
    }

    /** The brokers, node ids 1 to 3 in order. */
    public List<GenericContainer<?>> brokers() {
        return List.copyOf(brokers);
    }

    /** Whether the cluster keeps its metadata in ZooKeeper. */
    public boolean zookeeperMode() {
        return zookeeper != null;
    }

    /** The address a client outside Docker bootstraps from for broker {@code index}, counted from 0. */
    public String bootstrapServers(int index) {
        GenericContainer<?> broker = brokers.get(index);
        return broker instanceof ZooKeeperKafkaBroker zk
                ? zk.getBootstrapServers()
                : ((KafkaContainer) broker).getBootstrapServers();
    }

    /** The brokers' addresses as a client outside Docker reaches them. */
    public String bootstrapServers() {
        return IntStream.range(0, brokers.size())
                .mapToObj(this::bootstrapServers)
                .collect(Collectors.joining(","));
    }

    /** Whether the cluster has finalized {@code feature} at {@code level} or more, such as {@code share.version}. */
    public boolean supports(String feature, int level) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers()))) {
            FinalizedVersionRange finalized = admin.describeFeatures()
                    .featureMetadata()
                    .get()
                    .finalizedFeatures()
                    .get(feature);
            return finalized != null && finalized.maxVersionLevel() >= level;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    /** What a caller of the driver passes to reach this cluster. */
    public ClusterSpec spec() {
        List<NodeSpec> nodes = new ArrayList<>();
        for (int i = 0; i < brokers.size(); i++) {
            nodes.add(new NodeSpec(
                    NodeId.of(String.valueOf(i + 1)),
                    List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, bootstrapServers(i)))));
        }
        return new ClusterSpec(nodes, List.of(), Map.of());
    }

    @Override
    public void close() {
        brokers.forEach(GenericContainer::stop);
        if (zookeeper != null) {
            zookeeper.stop();
        }
        network.close();
    }
}
