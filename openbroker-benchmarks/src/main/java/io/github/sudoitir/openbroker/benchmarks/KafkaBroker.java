package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.util.List;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A single Kafka broker in Docker (the {@code kafka.image} system property, by default the newest the
 * driver is tested on) and a driver connection to it. Kafka has no embedded broker, so unlike
 * {@link EmbeddedBroker} the broker's own allocation is out of the measurement, which counts the
 * client only.
 */
final class KafkaBroker implements AutoCloseable {

    private final KafkaContainer broker;
    private final BrokerDrivers drivers;
    private final ClusterConnection connection;

    KafkaBroker() {
        broker = new KafkaContainer(System.getProperty("kafka.image", "apache/kafka:4.4.0"))
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
        broker.start();
        drivers = BrokerDrivers.load();
        connection = drivers.connect(new ConnectRequest(
                "kafka",
                ClusterSpec.ofNodes(List.of(new NodeSpec(
                        NodeId.of("1"), List.of(Endpoint.of(Plane.DATA, "kafka", broker.getBootstrapServers()))))),
                CredentialSource.none(),
                ConnectOptions.defaults()));
    }

    Transport transport() {
        return connection.transport().orElseThrow();
    }

    /** A new topic and the consumer group that starts at its beginning, as a subscription the transport can use. */
    ResourceId topic(String name) {
        ResourceId id = new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(name + "-group", name));
        connection
                .operations()
                .execute(id, new Operation.Create(Attributes.empty()), Scope.whole())
                .orThrow();
        return id;
    }

    @Override
    public void close() {
        connection.close();
        drivers.close();
        broker.stop();
    }
}
