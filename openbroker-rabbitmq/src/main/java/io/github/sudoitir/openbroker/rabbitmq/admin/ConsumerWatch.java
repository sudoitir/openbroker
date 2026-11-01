package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.notification.NotificationListener;

/**
 * Polls {@code /api/consumers} and emits {@code CONSUMER_ATTACHED} and {@code CONSUMER_DETACHED} with the
 * subscription attribute, beside the polling notifications of resources.
 */
final class ConsumerWatch implements AutoCloseable {

    ConsumerWatch(RabbitMqCluster cluster, NotificationListener listener) {}

    void start() {
        throw RabbitMqErrors.unimplemented("ConsumerWatch.start");
    }

    @Override
    public void close() {}
}
