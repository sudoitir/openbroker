package io.github.sudoitir.openbroker;

import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.operation.Operations;
import java.util.Optional;

/**
 * A connection to one cluster, offering one facet per concern. Thread-safe: share one per cluster.
 * Optional facets are empty when the system has no such thing.
 */
public interface ClusterConnection extends AutoCloseable {

    BrokerSystem system();

    Nodes nodes();

    Resources resources();

    Operations operations();

    Messages messages();

    Access access();

    Optional<Notifications> notifications();

    @Incubating
    Optional<Transport> transport();

    /**
     * Applies a changed cluster specification: new nodes become usable, removed nodes' receivers,
     * senders and subscriptions are closed, and unchanged nodes keep theirs.
     */
    void reconfigure(ClusterSpec cluster);

    /**
     * The driver's own interface for what only this system can do, when it offers {@code type}. This
     * is the one extension mechanism: native behaviour is never part of the neutral facets.
     */
    <T> Optional<T> unwrap(Class<T> type);

    /** Closes every receiver, sender and subscription of this connection. */
    @Override
    void close();
}
