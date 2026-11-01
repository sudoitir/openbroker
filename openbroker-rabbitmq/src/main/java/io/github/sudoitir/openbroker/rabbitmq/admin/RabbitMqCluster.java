package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The per-connection context shared by the facets: spec, credentials, connect options, the {@code ManagementClient},
 * the AMQP connections and the cached cluster features.
 */
public final class RabbitMqCluster implements AutoCloseable {

    /** The node id of calls that concern the whole cluster. */
    public static final NodeId CLUSTER = NodeId.of("cluster");

    public RabbitMqCluster(ClusterSpec spec, CredentialSource credentials, DriverContext context) {}

    /** Runs {@code work} as the named operation: admitted by the call interceptor, bounded by {@code deadline}. */
    public <T> T call(String operation, Optional<ResourceId> resource, Deadline deadline, Supplier<T> work) {
        throw RabbitMqErrors.unimplemented("RabbitMqCluster.call");
    }

    /** Applies a changed specification: drops the caches and closes the AMQP connections. */
    public void reconfigure(ClusterSpec spec) {
        throw RabbitMqErrors.unimplemented("RabbitMqCluster.reconfigure");
    }

    @Override
    public void close() {}
}
