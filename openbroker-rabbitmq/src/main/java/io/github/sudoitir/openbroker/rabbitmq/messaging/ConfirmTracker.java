package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks publisher confirms: a publish sequence number to its pending receipt; an ack (possibly multiple) settles it, a
 * nack fails it, a return before the ack fails it {@code NOT_ROUTED}, and a lost channel leaves the unconfirmed in doubt.
 */
final class ConfirmTracker {

    ConfirmTracker() {}

    void published(long sequence, CompletableFuture<SendReceipt> pending) {
        throw RabbitMqErrors.unimplemented("ConfirmTracker.published");
    }

    void ack(long sequence, boolean multiple) {
        throw RabbitMqErrors.unimplemented("ConfirmTracker.ack");
    }

    void nack(long sequence, boolean multiple) {
        throw RabbitMqErrors.unimplemented("ConfirmTracker.nack");
    }

    void returned(long sequence) {
        throw RabbitMqErrors.unimplemented("ConfirmTracker.returned");
    }

    void channelLost() {
        throw RabbitMqErrors.unimplemented("ConfirmTracker.channelLost");
    }
}
