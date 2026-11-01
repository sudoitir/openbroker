package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMq;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTestCluster;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * What the watch sees on a real node: the consumers that attach and detach where the event exchange does not exist,
 * the events of the event exchange where it does, and what the account may do in each case.
 */
class RabbitMqWatchIT {

    private static final String VHOST = "/";

    private static RabbitMqCluster cluster(RabbitMqTestCluster node) {
        ClusterSpec spec = node.spec();
        ClusterSpec fast =
                new ClusterSpec(spec.nodes(), spec.clusterEndpoints(), Map.of(RabbitMqOptions.WATCH_INTERVAL, "PT1S"));
        return new RabbitMqCluster(
                fast,
                CredentialSource.of(node.credentials()),
                new FakeManagement.TestContext(ConnectOptions.defaults()));
    }

    private static final class Collector implements NotificationListener {
        final BlockingQueue<BrokerEvent> events = new LinkedBlockingQueue<>();

        @Override
        public void onEvent(BrokerEvent event) {
            events.add(event);
        }

        @Override
        public void onGap(Gap gap) {
            // The start of the stream is not what these cases wait for.
        }

        BrokerEvent await(String type) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                BrokerEvent event = events.poll(1, TimeUnit.SECONDS);
                if (event != null && type.equals(event.type())) {
                    return event;
                }
            }
            throw new AssertionError("No " + type + " event arrived");
        }
    }

    /** One connection per call to a node of the cluster, enough for the stream under test. */
    private record DirectConnections(RabbitMqTestCluster node) implements AmqpConnections {
        @Override
        public Connection connection(String vhost, Deadline deadline) {
            try {
                RabbitMqCluster.HostPort address =
                        RabbitMqCluster.dataAddress(node.spec().nodes().getFirst().endpoints().stream()
                                .filter(e -> e.plane() == Plane.DATA)
                                .findFirst()
                                .orElseThrow());
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(address.host());
                factory.setPort(address.port());
                factory.setUsername(RabbitMqTestCluster.ADMIN_USER);
                factory.setPassword(RabbitMqTestCluster.ADMIN_PASSWORD);
                factory.setVirtualHost(vhost);
                return factory.newConnection("watch-it");
            } catch (Exception e) {
                throw RabbitMqErrors.fromIo(e, false);
            }
        }

        @Override
        public AutoCloseable onLoss(String vhost, Runnable listener) {
            return () -> {};
        }

        @Override
        public void reconfigure() {}

        @Override
        public void close() {}
    }

    @Test
    void theConsumerWatchSeesAConsumerAttachAndDetachWithoutTheEventExchange() throws Exception {
        try (RabbitMqTestCluster node = RabbitMqTestCluster.single()) {
            RabbitMqCluster cluster = cluster(node);
            RabbitMq.of(node, 0).declareQueue(VHOST, "watched", "quorum");
            Collector collector = new Collector();
            try (ConsumerWatch watch = new ConsumerWatch(cluster, collector)) {
                watch.start();
                ConnectionFactory factory = new ConnectionFactory();
                String[] address = node.amqpAddress(0).split(":");
                factory.setHost(address[0]);
                factory.setPort(Integer.parseInt(address[1]));
                factory.setUsername(RabbitMqTestCluster.ADMIN_USER);
                factory.setPassword(RabbitMqTestCluster.ADMIN_PASSWORD);
                try (Connection connection = factory.newConnection("watch-it-consumer")) {
                    Channel channel = connection.createChannel();
                    channel.basicConsume("watched", true, (tag, delivery) -> {}, tag -> {});
                    BrokerEvent attached = collector.await(EventTypes.CONSUMER_ATTACHED);
                    ResourceId queue = ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), VHOST, "watched");
                    assertThat(attached.data().get(StandardAttributes.SUBSCRIPTION))
                            .contains(queue);
                }
                collector.await(EventTypes.CONSUMER_DETACHED);
            }
        }
    }

    @Test
    void theEventExchangeStreamSeesAQueueBeingCreated() throws Exception {
        try (RabbitMqTestCluster node = RabbitMqTestCluster.single(RabbitMqTestCluster.EVENT_EXCHANGE_PLUGIN)) {
            RabbitMqCluster cluster = cluster(node);
            Collector collector = new Collector();
            try (DirectConnections connections = new DirectConnections(node);
                    EventExchangeStream stream = new EventExchangeStream(cluster, collector, connections)) {
                stream.start();
                RabbitMq.of(node, 0).declareQueue(VHOST, "announced", "stream");
                BrokerEvent created = collector.await(EventTypes.RESOURCE_CREATED);
                assertThat(created.subject()).contains(ResourceId.of(RabbitMqKinds.STREAM.value(), VHOST, "announced"));
            }
        }
    }

    @Test
    void accessNamesTheEventExchangeOnlyWhereThePluginIsEnabled() {
        try (RabbitMqTestCluster without = RabbitMqTestCluster.single();
                RabbitMqTestCluster with = RabbitMqTestCluster.single(RabbitMqTestCluster.EVENT_EXCHANGE_PLUGIN)) {
            Assessment missing =
                    new RabbitMqAccess(cluster(without)).check(RabbitMqAccess.EVENT_EXCHANGE, Optional.empty());
            assertThat(missing.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
            assertThat(missing.remedy()).isPresent();
            Assessment present =
                    new RabbitMqAccess(cluster(with)).check(RabbitMqAccess.EVENT_EXCHANGE, Optional.empty());
            assertThat(present.status()).isEqualTo(Assessment.Status.AVAILABLE);
            assertThat(new RabbitMqAccess(cluster(with))
                            .check(Capability.MANAGEMENT_WRITE, Optional.empty())
                            .status())
                    .isEqualTo(Assessment.Status.AVAILABLE);
        }
    }
}
