package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.artemis.ArtemisManagement;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.Optional;

/** One connection to an Artemis cluster. Thread-safe. */
public final class ArtemisConnection implements ClusterConnection {

    private final BrokerSystem system;
    private final Cluster cluster;
    private final ArtemisNodes nodes;
    private final ArtemisResources resources;
    private final ArtemisOperations operations;
    private final CorePool core;
    private final ArtemisMessages messages;
    private final ArtemisAccess access;

    public ArtemisConnection(
            BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
        this.cluster = new Cluster(spec, credentials, context);
        this.nodes = new ArtemisNodes(cluster);
        this.resources = new ArtemisResources(cluster);
        this.operations = new ArtemisOperations(cluster);
        this.core = new CorePool(cluster);
        this.messages = new ArtemisMessages(cluster, core);
        this.access = new ArtemisAccess(cluster);
    }

    @Override
    public BrokerSystem system() {
        return system;
    }

    @Override
    public Nodes nodes() {
        return nodes;
    }

    @Override
    public Resources resources() {
        return resources;
    }

    @Override
    public Operations operations() {
        return operations;
    }

    @Override
    public Messages messages() {
        return messages;
    }

    @Override
    public Access access() {
        return access;
    }

    @Override
    public Optional<Notifications> notifications() {
        return Optional.empty();
    }

    @Override
    public Optional<Transport> transport() {
        return Optional.empty();
    }

    @Override
    public void reconfigure(ClusterSpec spec) {
        cluster.reconfigure(spec);
    }

    @Override
    public <T> Optional<T> unwrap(Class<T> type) {
        if (type == ArtemisManagement.class) {
            ArtemisManagement management = node ->
                    new NodeJolokia(cluster.jolokia(cluster.node(node)), () -> Deadlines.current(cluster.options()));
            return Optional.of(type.cast(management));
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        core.close();
    }
}
