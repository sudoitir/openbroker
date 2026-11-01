package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.util.List;

/**
 * Builds the {@link ConnectionFactory} from the spec, the credentials and the TLS material, and the list of data
 * addresses (cluster endpoints first, then node endpoints).
 */
public final class AmqpFactory {

    public AmqpFactory(RabbitMqCluster cluster) {}

    /** A factory for {@code vhost}: recovery off, named {@code openbroker-<connection id>-<vhost>}. */
    public ConnectionFactory factory(String vhost) {
        throw RabbitMqErrors.unimplemented("AmqpFactory.factory");
    }

    public List<Address> addresses() {
        throw RabbitMqErrors.unimplemented("AmqpFactory.addresses");
    }
}
