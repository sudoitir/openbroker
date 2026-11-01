package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.tck.OpenBrokerTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;

/** The TCK against the three-node cluster, on quorum queues (the kind RabbitMQ 4 declares by default). */
class RabbitMqTckIT extends OpenBrokerTck {

    @Override
    protected TckEnvironment environment() {
        return new RabbitMqTckEnvironment(RabbitMqKinds.QUORUM_QUEUE);
    }
}
