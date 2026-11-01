package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;

/** The {@link ProcessorEngine.Lanes} over {@code RabbitMqReceiver}s: each lane holds at most one unsettled message. */
final class RabbitMqLanes implements ProcessorEngine.Lanes, AutoCloseable {

    RabbitMqLanes(
            RabbitMqCluster cluster,
            String processorId,
            ResourceId subscription,
            ProcessorOptions options,
            DeadLetterSink deadLetters) {}

    @Override
    public ProcessorEngine.Lane open(int index) {
        throw RabbitMqErrors.unimplemented("RabbitMqLanes.open");
    }

    @Override
    public boolean pinsGroups() {
        throw RabbitMqErrors.unimplemented("RabbitMqLanes.pinsGroups");
    }

    @Override
    public LinkState state() {
        throw RabbitMqErrors.unimplemented("RabbitMqLanes.state");
    }

    @Override
    public Sender deadLetter(SendTarget target) {
        throw RabbitMqErrors.unimplemented("RabbitMqLanes.deadLetter");
    }

    @Override
    public void close() {}
}
