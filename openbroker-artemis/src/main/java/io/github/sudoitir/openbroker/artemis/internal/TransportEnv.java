package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.messaging.EngineContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** What the links of one connection's transport share. */
final class TransportEnv implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(TransportEnv.class.getName());
    private static final String SYSTEM = "artemis";

    private final Cluster cluster;
    private final CoreLinks links;
    private final HaGroups groups;
    private final Addresses addresses;
    private final EngineContext engine;
    private final CopyOnWriteArrayList<LinkListener> listeners = new CopyOnWriteArrayList<>();
    private final Set<AutoCloseable> open = ConcurrentHashMap.newKeySet();

    TransportEnv(Cluster cluster, CoreLinks links, HaGroups groups, Addresses addresses) {
        this.cluster = cluster;
        this.links = links;
        this.groups = groups;
        this.addresses = addresses;
        this.engine = EngineContext.of(cluster.options());
        if (cluster.options().observer() instanceof LinkListener observer) {
            listeners.add(observer);
        }
    }

    Cluster cluster() {
        return cluster;
    }

    CoreLinks links() {
        return links;
    }

    HaGroups groups() {
        return groups;
    }

    Addresses addresses() {
        return addresses;
    }

    EngineContext engine() {
        return engine;
    }

    SpoolDirectory spool() {
        return new SpoolDirectory(cluster.options().spoolDirectory());
    }

    /** Hears the state changes of every link until the result is closed. */
    AutoCloseable watch(LinkListener listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    /** Passes one link's event to everyone who watches. */
    void fire(LinkEvent event) {
        for (LinkListener listener : listeners) {
            try {
                listener.changed(event);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "A link listener failed", e);
            }
        }
    }

    /** Remembers a link so closing the connection closes it. */
    <T extends AutoCloseable> T track(T link) {
        open.add(link);
        return link;
    }

    void forget(AutoCloseable link) {
        open.remove(link);
    }

    /** The telemetry description of one operation on a destination. */
    OperationInfo info(
            OperationType type, ResourceId destination, Optional<ResourceId> subscription, NodeHandle node, int count) {
        Optional<String> host = Optional.empty();
        OptionalInt port = OptionalInt.empty();
        if (node.core().isPresent()) {
            try {
                URI uri = URI.create(CoreLinks.dialable(node.core().get()));
                host = Optional.ofNullable(uri.getHost());
                port = uri.getPort() >= 0 ? OptionalInt.of(uri.getPort()) : OptionalInt.empty();
            } catch (IllegalArgumentException e) {
                // an endpoint the URI parser does not read is reported without host and port
            }
        }
        return new OperationInfo(
                SYSTEM,
                type,
                type.name().toLowerCase(Locale.ROOT),
                destination,
                subscription,
                node.id(),
                host,
                port,
                count);
    }

    @Override
    public void close() {
        List<AutoCloseable> all = new ArrayList<>(open);
        for (AutoCloseable link : all) {
            try {
                link.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.DEBUG, "A link failed to close", e);
            }
        }
        open.clear();
    }
}
