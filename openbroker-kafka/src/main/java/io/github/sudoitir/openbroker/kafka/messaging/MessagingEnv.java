package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.messaging.EngineContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/** What the links of one connection's transport share: the cluster, the engine context, the listeners. */
final class MessagingEnv implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(MessagingEnv.class.getName());

    private final KafkaCluster cluster;
    private final EngineContext engine;
    private final CopyOnWriteArrayList<LinkListener> listeners = new CopyOnWriteArrayList<>();
    private final Set<AutoCloseable> open = ConcurrentHashMap.newKeySet();

    MessagingEnv(KafkaCluster cluster) {
        this.cluster = cluster;
        this.engine = EngineContext.of(cluster.options());
        if (cluster.options().observer() instanceof LinkListener observer) {
            listeners.add(observer);
        }
    }

    KafkaCluster cluster() {
        return cluster;
    }

    EngineContext engine() {
        return engine;
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

    /**
     * Runs {@code work} admitted by the caller's interceptor as a data-plane call named {@code
     * operation}, mapping what a Kafka client throws to the SDK's failures.
     */
    <T> T admitted(String operation, ResourceId target, Supplier<T> work) {
        CallInfo call = new CallInfo(
                KafkaCluster.CLUSTER, cluster.bootstrapServers(), Plane.DATA, operation, 1, Optional.of(target));
        try (Permit permit = cluster.options().interceptor().admit(call)) {
            try {
                T result = work.get();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (RuntimeException e) {
                BrokerException failure = KafkaFailures.of(e, Optional.of(target));
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            }
        }
    }

    /** The telemetry description of one operation on a destination. */
    OperationInfo info(OperationType type, ResourceId destination, Optional<ResourceId> subscription, int count) {
        String first = cluster.bootstrapServers().split(",")[0];
        int colon = first.lastIndexOf(':');
        Optional<String> host = colon > 0 ? Optional.of(first.substring(0, colon)) : Optional.empty();
        OptionalInt port = OptionalInt.empty();
        if (colon > 0) {
            try {
                port = OptionalInt.of(Integer.parseInt(first.substring(colon + 1)));
            } catch (NumberFormatException e) {
                // an address without a numeric port is reported without one
            }
        }
        return new OperationInfo(
                KafkaDriver.SYSTEM,
                type,
                type.name().toLowerCase(Locale.ROOT),
                destination,
                subscription,
                KafkaCluster.CLUSTER,
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
