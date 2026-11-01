package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.time.Duration;
import java.util.Optional;

/**
 * {@link Sender} on the send pipeline: one confirm-mode channel per link, a {@code mandatory} publish, and confirms and
 * returns settled through the {@link ConfirmTracker}.
 */
final class RabbitMqSender implements Sender {

    RabbitMqSender(RabbitMqCluster cluster, SendTarget target, SenderOptions options) {}

    @Override
    public String id() {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.id");
    }

    @Override
    public LinkState state() {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.state");
    }

    @Override
    public Optional<DeduplicationWindow> deduplication() {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.deduplication");
    }

    @Override
    public SendReceipt send(OutboundMessage message) {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.send");
    }

    @Override
    public Offer offer(OutboundMessage message) {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.offer");
    }

    @Override
    public void flush(Duration timeout) {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.flush");
    }

    @Override
    public void commit() {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.commit");
    }

    @Override
    public void rollback() {
        throw RabbitMqErrors.unimplemented("RabbitMqSender.rollback");
    }

    @Override
    public void close() {}
}
