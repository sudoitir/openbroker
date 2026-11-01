package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;

/**
 * One {@link Connection} per virtual host, opened on first use within the caller's deadline and kept until the
 * cluster specification changes or the cluster closes.
 *
 * <p>The client's automatic recovery is off (ADR-0011): a connection that dies is dropped, the {@code onLoss}
 * listeners of its virtual host run, and the next {@link #connection} call opens a new one with the current
 * specification, credentials and TLS material. A close that this class asked for is not a loss.
 */
public final class PooledAmqpConnections implements AmqpConnections {

    private static final long MIN_TIMEOUT_MILLIS = 1;
    private static final int CLOSE_OK = 200;
    private static final int CLOSE_TIMEOUT_MILLIS = 1000;

    /** A virtual host's connection and the lock that serialises opening it. */
    private static final class Slot {
        private @Nullable Connection connection;
    }

    private final RabbitMqCluster cluster;
    private final AmqpFactory factory;
    private final Map<String, Slot> slots = new ConcurrentHashMap<>();
    private final Map<String, List<Runnable>> listeners = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /** Connections of {@code cluster}, closed when it is reconfigured or closed. */
    public PooledAmqpConnections(RabbitMqCluster cluster) {
        this(cluster, new AmqpFactory(cluster));
    }

    PooledAmqpConnections(RabbitMqCluster cluster, AmqpFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
        cluster.onReconfigure(this::reconfigure);
        cluster.onClose(this::close);
    }

    @Override
    public Connection connection(String vhost, Deadline deadline) {
        if (closed) {
            throw RabbitMqErrors.invalid("The RabbitMQ connection is closed.");
        }
        ConnectOptions options = cluster.options();
        Slot slot = slots.computeIfAbsent(vhost, v -> new Slot());
        synchronized (slot) {
            Connection open = slot.connection;
            if (open != null && open.isOpen()) {
                return open;
            }
            if (deadline.expired(options.clock())) {
                throw Deadlines.exceeded(RabbitMqCluster.CLUSTER, Dispatch.NOT_SENT);
            }
            Connection opened = open(vhost, deadline, options);
            slot.connection = opened;
            opened.addShutdownListener(cause -> lost(vhost, slot, opened, cause));
            return opened;
        }
    }

    @Override
    public AutoCloseable onLoss(String vhost, Runnable listener) {
        List<Runnable> list = listeners.computeIfAbsent(vhost, v -> new CopyOnWriteArrayList<>());
        list.add(listener);
        return () -> list.remove(listener);
    }

    @Override
    public void reconfigure() {
        closeAll();
    }

    @Override
    public void close() {
        closed = true;
        closeAll();
        listeners.clear();
    }

    private Connection open(String vhost, Deadline deadline, ConnectOptions options) {
        ConnectionFactory connectionFactory = factory.factory(vhost);
        Clock clock = options.clock();
        Duration remaining = deadline.remaining(clock);
        Duration connect = options.timeouts().connect();
        long millis = Math.max(MIN_TIMEOUT_MILLIS, (remaining.compareTo(connect) < 0 ? remaining : connect).toMillis());
        int timeout = (int) Math.min(Integer.MAX_VALUE, millis);
        connectionFactory.setConnectionTimeout(timeout);
        connectionFactory.setHandshakeTimeout(timeout);
        try {
            return connectionFactory.newConnection(factory.addresses(), factory.name(vhost));
        } catch (IOException | TimeoutException e) {
            throw RabbitMqErrors.fromIo(e, false);
        }
    }

    private void lost(String vhost, Slot slot, Connection connection, ShutdownSignalException cause) {
        synchronized (slot) {
            if (slot.connection == connection) {
                slot.connection = null;
            }
        }
        if (!cause.isInitiatedByApplication()) {
            for (Runnable listener : listeners.getOrDefault(vhost, List.of())) {
                listener.run();
            }
        }
    }

    private void closeAll() {
        for (Slot slot : List.copyOf(slots.values())) {
            Connection connection;
            synchronized (slot) {
                connection = slot.connection;
                slot.connection = null;
            }
            if (connection != null) {
                closeQuietly(connection);
            }
        }
        slots.clear();
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close(CLOSE_OK, "closed by the application", CLOSE_TIMEOUT_MILLIS);
        } catch (IOException | ShutdownSignalException e) {
            // Already closed or unreachable: nothing is left to release.
        }
    }
}
