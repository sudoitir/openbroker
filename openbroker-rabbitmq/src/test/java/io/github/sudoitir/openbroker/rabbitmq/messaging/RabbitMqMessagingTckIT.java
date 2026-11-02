package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTckEnvironment;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.tck.MessagingTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;

/** The messaging TCK against the three-node cluster, on quorum queues. */
class RabbitMqMessagingTckIT extends MessagingTck {

    @Override
    protected TckEnvironment environment() {
        return new RabbitMqTckEnvironment(RabbitMqKinds.QUORUM_QUEUE);
    }
}
