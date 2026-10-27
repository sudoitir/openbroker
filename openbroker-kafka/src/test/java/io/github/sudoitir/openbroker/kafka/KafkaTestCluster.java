package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

/**
 * A three-broker KRaft cluster in Docker, the one every Kafka integration test runs against. Each
 * broker is also a controller (combined roles), topics have three replicas and need two in sync,
 * and an out-of-sync replica can never become leader, so a test that stops a broker sees what a
 * production cluster would do. The image is the {@code kafka.image} system property, by default the
 * newest the driver is tested on.
 */
public final class KafkaTestCluster implements AutoCloseable {

    /** The system property that names the image, such as {@code apache/kafka:4.0.2}. */
    public static final String IMAGE_PROPERTY = "kafka.image";

    public static final String DEFAULT_IMAGE = "apache/kafka:4.4.0";

    private static final int BROKERS = 3;
    private static final int CONTROLLER_PORT = 9094;

    private final Network network = Network.newNetwork();
    private final List<KafkaContainer> brokers = new ArrayList<>();

    private KafkaTestCluster(String image) {
        String voters = IntStream.rangeClosed(1, BROKERS)
                .mapToObj(id -> id + "@" + host(id) + ":" + CONTROLLER_PORT)
                .collect(Collectors.joining(","));
        for (int id = 1; id <= BROKERS; id++) {
            String host = host(id);
            brokers.add(new KafkaContainer(image)
                    .withNetwork(network)
                    .withNetworkAliases(host)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(host))
                    .withEnv(environment(id, voters)));
        }
    }

    /** Starts the cluster on the image the build names. */
    public static KafkaTestCluster start() {
        return start(System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE));
    }

    /** Starts the three brokers together, because a controller quorum needs a majority to elect. */
    public static KafkaTestCluster start(String image) {
        KafkaTestCluster cluster = new KafkaTestCluster(image);
        try {
            Startables.deepStart(cluster.brokers).join();
        } catch (RuntimeException e) {
            cluster.close();
            throw e;
        }
        return cluster;
    }

    private static String host(int id) {
        return "kafka-" + id;
    }

    private static Map<String, String> environment(int id, String voters) {
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
                // Three brokers on one CI runner: small heaps keep them from starving each other.
                Map.entry("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m"));
    }

    /** The brokers, node ids 1 to 3 in order. */
    public List<KafkaContainer> brokers() {
        return List.copyOf(brokers);
    }

    /** The brokers' addresses as a client outside Docker reaches them. */
    public String bootstrapServers() {
        return brokers.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
    }

    /** What a caller of the driver passes to reach this cluster. */
    public ClusterSpec spec() {
        return new ClusterSpec(
                List.of(),
                brokers.stream()
                        .map(b -> Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, b.getBootstrapServers()))
                        .toList(),
                Map.of());
    }

    @Override
    public void close() {
        brokers.forEach(KafkaContainer::stop);
        network.close();
    }
}
