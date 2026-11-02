package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.messaging.EngineContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/** A cluster, connections and channels that are mocks, and an engine context that never sleeps. */
final class MessagingFixtures {

    final List<Channel> channels = new CopyOnWriteArrayList<>();
    final AmqpConnections connections = mock(AmqpConnections.class);
    final Connection connection = mock(Connection.class);
    final RabbitMqCluster cluster;
    final LinkEnv env;

    MessagingFixtures() {
        this(ConnectOptions.defaults());
    }

    MessagingFixtures(ConnectOptions options) {
        this.cluster = new RabbitMqCluster(
                new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.DATA, "amqp", "rabbit:5672")), Map.of()),
                CredentialSource.of(Credentials.usernamePassword("u", "p")),
                new Context(options));
        EngineContext real = EngineContext.of(options);
        EngineContext engine = new EngineContext(
                real.clock(),
                real.random(),
                duration -> {},
                real.contextPropagation(),
                real.observer(),
                real.interceptor());
        try {
            when(connections.connection(any(), any())).thenReturn(connection);
            when(connection.createChannel()).thenAnswer(call -> {
                Channel channel = mock(Channel.class);
                when(channel.isOpen()).thenReturn(true);
                when(channel.getNextPublishSeqNo()).thenReturn(1L);
                channels.add(channel);
                return channel;
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        this.env = new LinkEnv(cluster, connections, engine);
    }

    /** Waits until a link event with {@code state} arrives. */
    static CountDownLatch awaiting(LinkEnv env, LinkState state) {
        CountDownLatch latch = new CountDownLatch(1);
        env.watch((LinkEvent event) -> {
            if (event.state() == state) {
                latch.countDown();
            }
        });
        return latch;
    }

    static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out");
        }
    }

    /** The channel opened at {@code index}, waiting until it exists. */
    Channel channel(int index) {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (channels.size() <= index) {
            if (System.nanoTime() > end) {
                throw new AssertionError("no channel " + index);
            }
            LockSupport.parkNanos(1_000_000);
        }
        return channels.get(index);
    }

    private record Context(ConnectOptions options) implements DriverContext {
        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return factory.get();
        }

        @Override
        public void evict(String key) {
            // Nothing is kept.
        }
    }
}
