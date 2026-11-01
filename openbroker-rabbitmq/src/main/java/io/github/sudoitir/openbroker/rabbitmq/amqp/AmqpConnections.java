package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.Connection;
import io.github.sudoitir.openbroker.Deadline;

/**
 * One lazily opened AMQP 0-9-1 {@link Connection} per virtual host, opened under a deadline, closed on
 * reconfiguration and on close, and reporting a connection loss to the links on it.
 */
public interface AmqpConnections extends AutoCloseable {

    /** The open connection to {@code vhost}, opened first if need be within {@code deadline}. */
    Connection connection(String vhost, Deadline deadline);

    /**
     * Calls {@code listener} when the connection to {@code vhost} is lost.
     *
     * @return a handle whose close removes the listener
     */
    AutoCloseable onLoss(String vhost, Runnable listener);

    /** Closes every connection, as the cluster specification changed. */
    void reconfigure();

    @Override
    void close();
}
