package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.notification.NotificationListener;

/**
 * Reads {@code amq.rabbitmq.event} through an exclusive server-named queue bound with {@code #} in the event virtual
 * host, maps the events, and reports a gap ({@code STARTED}, then {@code RECONNECTED} after every reopen).
 */
final class EventExchangeStream implements AutoCloseable {

    EventExchangeStream(RabbitMqCluster cluster, NotificationListener listener) {}

    void start() {
        throw RabbitMqErrors.unimplemented("EventExchangeStream.start");
    }

    @Override
    public void close() {}
}
