package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * One three-broker cluster for every messaging test of the JVM, started on first use: starting
 * three brokers costs more than most tests. Topics and groups are unique per test.
 */
final class SharedKafka {

    private static KafkaTestCluster cluster;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    private SharedKafka() {}

    static synchronized KafkaTestCluster cluster() {
        if (cluster == null) {
            cluster = KafkaTestCluster.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (drivers != null) {
                    drivers.close();
                }
                cluster.close();
            }));
        }
        return cluster;
    }

    /** A connection every test shares; it holds no state a test depends on. */
    static synchronized ClusterConnection connection() {
        if (connection == null) {
            drivers = BrokerDrivers.load();
            connection = drivers.connect(request(ConnectOptions.defaults()));
        }
        return connection;
    }

    /** A cluster handle of its own, for tests that reach below the transport. */
    static MessagingEnv env() {
        DriverContext context = new DriverContext() {
            @Override
            public ConnectOptions options() {
                return ConnectOptions.defaults();
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
        return new MessagingEnv(new KafkaCluster(cluster().spec(), CredentialSource.none(), context));
    }

    static Transport transport() {
        return connection().transport().orElseThrow();
    }

    static ConnectRequest request(ConnectOptions options) {
        return new ConnectRequest(KafkaDriver.SYSTEM, cluster().spec(), CredentialSource.none(), options);
    }

    /** Creates a topic with a unique name and the given partitions, replicated three times. */
    static String topic(String tag, int partitions) {
        return create("t." + tag + "." + UUID.randomUUID().toString().substring(0, 8), partitions);
    }

    /** Creates a topic named {@code name}, replicated three times. */
    static String create(String name, int partitions) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", cluster().bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 3)))
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not create " + name, e);
        }
        return name;
    }

    /** A unique group on {@code topic}. */
    static ResourceId group(String topic) {
        return ResourceId.of(
                "kafka.group-topic", "g." + UUID.randomUUID().toString().substring(0, 8), topic);
    }

    static ResourceId topicId(String topic) {
        return ResourceId.of("kafka.topic", topic);
    }
}
