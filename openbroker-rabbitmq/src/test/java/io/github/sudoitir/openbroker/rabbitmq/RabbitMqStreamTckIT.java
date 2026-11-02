package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.tck.OpenBrokerTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;

/** The TCK against the three-node cluster, on streams, whose messages can be browsed. */
class RabbitMqStreamTckIT extends OpenBrokerTck {

    @Override
    protected TckEnvironment environment() {
        return new RabbitMqTckEnvironment(RabbitMqKinds.STREAM);
    }
}
