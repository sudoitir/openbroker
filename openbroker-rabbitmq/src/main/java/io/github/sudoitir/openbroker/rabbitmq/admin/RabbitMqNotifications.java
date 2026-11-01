package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.notification.StreamState;
import java.util.Map;

/**
 * {@link Notifications}: chooses the event-exchange stream or the polling watch for each subscription; {@code state()}
 * is keyed by {@link RabbitMqCluster#CLUSTER}.
 */
final class RabbitMqNotifications implements Notifications, AutoCloseable {

    RabbitMqNotifications(RabbitMqCluster cluster, RabbitMqResources resources) {}

    @Override
    public AutoCloseable subscribe(Scope scope, NotificationListener listener) {
        throw RabbitMqErrors.unimplemented("RabbitMqNotifications.subscribe");
    }

    @Override
    public Map<NodeId, StreamState> state() {
        throw RabbitMqErrors.unimplemented("RabbitMqNotifications.state");
    }

    void reset() {
        throw RabbitMqErrors.unimplemented("RabbitMqNotifications.reset");
    }

    @Override
    public void close() {}
}
