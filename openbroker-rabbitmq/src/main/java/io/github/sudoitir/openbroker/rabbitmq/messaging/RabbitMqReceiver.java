package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.time.Duration;
import java.util.List;

/**
 * Pull {@link Receiver} on {@code basicConsume} with manual acknowledgement, {@code basicQos} from the flow control, a
 * local buffer, settlement, and a delayed release that is held and then rejected.
 */
final class RabbitMqReceiver implements Receiver {

    RabbitMqReceiver(RabbitMqCluster cluster, ResourceId subscription, ReceiverOptions options) {}

    @Override
    public String id() {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.id");
    }

    @Override
    public LinkState state() {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.state");
    }

    @Override
    public List<Message> receive(int max, Duration timeout) {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.receive");
    }

    @Override
    public void settle(Message message, Disposition disposition) {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.settle");
    }

    @Override
    public Sender sender(SendTarget target) {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.sender");
    }

    @Override
    public void commit() {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.commit");
    }

    @Override
    public void rollback() {
        throw RabbitMqErrors.unimplemented("RabbitMqReceiver.rollback");
    }

    @Override
    public void close() {}
}
