package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.AMQP;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;

/** Deadline-bounded control calls on a short-lived channel: passive declares of queues and exchanges. */
public final class AmqpCalls {

    public AmqpCalls(AmqpConnections connections) {}

    /** Declares {@code queue} passively: it fails when the queue does not exist. */
    public AMQP.Queue.DeclareOk queueDeclarePassive(String vhost, String queue, Deadline deadline) {
        throw RabbitMqErrors.unimplemented("AmqpCalls.queueDeclarePassive");
    }

    /** Declares {@code exchange} passively: it fails when the exchange does not exist. */
    public void exchangeDeclarePassive(String vhost, String exchange, Deadline deadline) {
        throw RabbitMqErrors.unimplemented("AmqpCalls.exchangeDeclarePassive");
    }
}
