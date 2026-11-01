package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/** Deadline-bounded control calls on a short-lived channel: passive declares of queues and exchanges. */
public final class AmqpCalls {

    private final AmqpConnections connections;
    private final Clock clock;

    /** Calls over {@code connections}, with deadlines read on the system clock. */
    public AmqpCalls(AmqpConnections connections) {
        this(connections, Clock.systemUTC());
    }

    /** Calls over {@code connections}, with deadlines read on {@code clock}. */
    public AmqpCalls(AmqpConnections connections, Clock clock) {
        this.connections = connections;
        this.clock = clock;
    }

    /**
     * Declares {@code queue} passively: it fails when the queue does not exist.
     *
     * @throws BrokerException {@code NOT_FOUND}/{@code RESOURCE_GONE} when it does not exist
     */
    public AMQP.Queue.DeclareOk queueDeclarePassive(String vhost, String queue, Deadline deadline) {
        return call(vhost, deadline, Optional.empty(), channel -> channel.queueDeclarePassive(queue));
    }

    /**
     * Declares {@code exchange} passively: it fails when the exchange does not exist.
     *
     * @throws BrokerException {@code NOT_FOUND}/{@code RESOURCE_GONE} when it does not exist
     */
    public void exchangeDeclarePassive(String vhost, String exchange, Deadline deadline) {
        Optional<ResourceId> resource = Optional.of(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost, exchange));
        call(vhost, deadline, resource, channel -> channel.exchangeDeclarePassive(exchange));
    }

    @FunctionalInterface
    private interface ChannelCall<T> {
        T run(Channel channel) throws IOException;
    }

    private <T> T call(String vhost, Deadline deadline, Optional<ResourceId> resource, ChannelCall<T> work) {
        if (deadline.expired(clock)) {
            throw Deadlines.exceeded(RabbitMqCluster.CLUSTER, Dispatch.NOT_SENT);
        }
        Connection connection = connections.connection(vhost, deadline);
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw failure(e, resource);
        }
        try {
            return work.run(channel);
        } catch (IOException e) {
            throw failure(e, resource);
        } finally {
            closeQuietly(channel);
        }
    }

    private static BrokerException failure(IOException e, Optional<ResourceId> resource) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ShutdownSignalException shutdown
                    && shutdown.getReason() instanceof AMQP.Channel.Close close) {
                return RabbitMqErrors.fromReply(close.getReplyCode(), close.getReplyText(), resource);
            }
        }
        return RabbitMqErrors.fromIo(e, false);
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException | ShutdownSignalException _) {
            // The channel is gone either way.
        }
    }
}
