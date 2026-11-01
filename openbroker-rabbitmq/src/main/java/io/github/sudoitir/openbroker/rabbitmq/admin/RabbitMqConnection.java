package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.Optional;

/**
 * The {@link ClusterConnection} to one RabbitMQ cluster: wires the facets; {@code notifications()} is always present,
 * {@code transport()} only with a {@code DATA} endpoint, and {@code unwrap} offers the {@code ManagementClient} and,
 * with a {@code DATA} endpoint, the AMQP {@code ConnectionFactory}. Thread-safe.
 */
public final class RabbitMqConnection implements ClusterConnection {

    private final BrokerSystem system;

    public RabbitMqConnection(
            BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
    }

    /** Reaches the cluster once: describes its nodes and probes what the account may do. */
    public ConnectionReport verify() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.verify");
    }

    @Override
    public BrokerSystem system() {
        return system;
    }

    @Override
    public Nodes nodes() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.nodes");
    }

    @Override
    public Resources resources() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.resources");
    }

    @Override
    public Operations operations() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.operations");
    }

    @Override
    public Messages messages() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.messages");
    }

    @Override
    public Access access() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.access");
    }

    @Override
    public Optional<Notifications> notifications() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.notifications");
    }

    @Override
    public Optional<Transport> transport() {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.transport");
    }

    @Override
    public void reconfigure(ClusterSpec cluster) {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.reconfigure");
    }

    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        throw RabbitMqErrors.unimplemented("RabbitMqConnection.unwrap");
    }

    @Override
    public void close() {}
}
