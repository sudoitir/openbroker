package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;

/**
 * {@link Transport} over AMQP 0-9-1: static {@code capabilities()}, senders, receivers and processors, and a link
 * supervisor per link.
 */
public final class RabbitMqTransport implements Transport, AutoCloseable {

    public RabbitMqTransport(RabbitMqCluster cluster) {}

    @Override
    public TransportCapabilities capabilities() {
        throw RabbitMqErrors.unimplemented("RabbitMqTransport.capabilities");
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        throw RabbitMqErrors.unimplemented("RabbitMqTransport.sender");
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        throw RabbitMqErrors.unimplemented("RabbitMqTransport.receiver");
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler) {
        throw RabbitMqErrors.unimplemented("RabbitMqTransport.process");
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        throw RabbitMqErrors.unimplemented("RabbitMqTransport.watch");
    }

    @Override
    public void close() {}
}
