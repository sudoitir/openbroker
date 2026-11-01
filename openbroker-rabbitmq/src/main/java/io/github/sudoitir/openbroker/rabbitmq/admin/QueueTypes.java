package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.KindId;

/**
 * A per-virtual-host map from queue name to queue type from a reduced listing, kept for the watch interval, so
 * bindings, consumers and events can name a queue's kind.
 */
final class QueueTypes {

    QueueTypes(RabbitMqCluster cluster) {}

    KindId kindOf(String vhost, String queue) {
        throw RabbitMqErrors.unimplemented("QueueTypes.kindOf");
    }

    void reset() {
        throw RabbitMqErrors.unimplemented("QueueTypes.reset");
    }
}
