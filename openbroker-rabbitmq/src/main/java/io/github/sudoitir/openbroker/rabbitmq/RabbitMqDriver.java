package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqConnection;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The RabbitMQ driver, for RabbitMQ 3.13 and later. The cluster is read and changed through the management HTTP API
 * ({@code MANAGEMENT} endpoints with protocol {@code http} or {@code https}) and messages move over AMQP 0-9-1
 * ({@code DATA} endpoints with protocol {@code amqp} or {@code amqps}, optional). Its nodes are the cluster's
 * RabbitMQ nodes.
 */
public final class RabbitMqDriver implements BrokerDriver {

    /** The system id, and the prefix of every kind this driver defines. */
    public static final String SYSTEM_ID = "rabbitmq";

    private static final BrokerSystem DESCRIPTION =
            new BrokerSystem(SYSTEM_ID, "RabbitMQ", "[3.13.0,)", 1, Set.of("com.rabbitmq."), List.of());

    @Override
    public BrokerSystem system() {
        return DESCRIPTION;
    }

    @Override
    public ConnectionSchema schema() {
        return RabbitMqOptions.SCHEMA;
    }

    @Override
    public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        requireShape(cluster);
        try (RabbitMqConnection connection = new RabbitMqConnection(DESCRIPTION, cluster, credentials, context)) {
            return connection.verify();
        }
    }

    @Override
    public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        requireShape(cluster);
        return new RabbitMqConnection(DESCRIPTION, cluster, credentials, context);
    }

    /**
     * Checks the endpoints and options of {@code cluster}: at least one management endpoint, every management
     * endpoint {@code http} or {@code https}, every data endpoint {@code amqp} or {@code amqps}, and valid options.
     *
     * @throws BrokerException {@code INVALID_ARGUMENT}, request not sent
     */
    static void requireShape(ClusterSpec cluster) {
        List<Endpoint> all = new ArrayList<>(cluster.clusterEndpoints());
        for (NodeSpec node : cluster.nodes()) {
            all.addAll(node.endpoints());
        }
        if (all.stream().noneMatch(e -> e.plane() == Plane.MANAGEMENT)) {
            throw refused("Add a management endpoint (http or https): RabbitMQ is read and changed through it.");
        }
        for (Endpoint endpoint : all) {
            Set<String> allowed = endpoint.plane() == Plane.MANAGEMENT
                    ? Set.of(RabbitMqOptions.HTTP, RabbitMqOptions.HTTPS)
                    : Set.of(RabbitMqOptions.AMQP, RabbitMqOptions.AMQPS);
            if (!allowed.contains(endpoint.protocol())) {
                throw refused("A " + endpoint.plane() + " endpoint is "
                        + String.join(" or ", allowed.stream().sorted().toList())
                        + ", not '" + endpoint.protocol() + "'.");
            }
        }
        RabbitMqOptions.watchInterval(cluster);
    }

    private static BrokerException refused(String message) {
        return new BrokerException(
                Status.INVALID_ARGUMENT, ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT), Dispatch.NOT_SENT, null, message);
    }
}
