package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.notification.BrokerEvent;
import java.util.Map;
import java.util.Optional;

/** Pure mapping of an event exchange message (routing key and headers) to a {@link BrokerEvent}. */
final class EventMapping {

    private EventMapping() {}

    static Optional<BrokerEvent> map(String routingKey, Map<String, Object> headers) {
        throw RabbitMqErrors.unimplemented("EventMapping.map");
    }
}
