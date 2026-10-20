package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
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

/** One connection to an Artemis cluster. Thread-safe. */
public final class ArtemisConnection implements ClusterConnection {

    private final BrokerSystem system;
    private final Cluster cluster;
    private final ArtemisNodes nodes;
    private final ArtemisResources resources;

    public ArtemisConnection(BrokerSystem system, ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.system = system;
        this.cluster = new Cluster(spec, credentials, context);
        this.nodes = new ArtemisNodes(cluster);
        this.resources = new ArtemisResources(cluster);
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
        throw Unimplemented.of("operate on resources through this driver yet");
    }

    @Override
    public Messages messages() {
        throw Unimplemented.of("handle messages through this driver yet");
    }

    @Override
    public Access access() {
        throw Unimplemented.of("assess access through this driver yet");
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
        return Optional.empty();
    }

    @Override
    public void close() {}
}
