package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.support.PollingNotifications;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * {@link Notifications}: chooses the event-exchange stream or the polling watch for each subscription; {@code state()}
 * is keyed by {@link RabbitMqCluster#CLUSTER}.
 *
 * <p>The event exchange is used when its plugin is enabled, the connection has a data endpoint and the account may
 * bind a queue to it; otherwise the cluster is read every {@code rabbitmq.watch_interval}, and
 * {@link ConsumerWatch} adds the consumers that attach and detach. The choice is made at each subscribe.
 */
final class RabbitMqNotifications implements Notifications, AutoCloseable {

    /** How many events a subscriber may fall behind by before it is told it missed some. */
    private static final int BUFFER = 1024;

    private final RabbitMqCluster cluster;
    private final RabbitMqResources resources;
    private final Optional<AmqpConnections> connections;
    private final Set<Subscription> open = ConcurrentHashMap.newKeySet();
    private volatile Notifications polling;

    /** Notifications that poll only, for a connection with no data endpoint. */
    RabbitMqNotifications(RabbitMqCluster cluster, RabbitMqResources resources) {
        this(cluster, resources, Optional.empty());
    }

    /** Notifications that read the event exchange over {@code connections} when they may, else poll. */
    RabbitMqNotifications(RabbitMqCluster cluster, RabbitMqResources resources, Optional<AmqpConnections> connections) {
        this.cluster = cluster;
        this.resources = resources;
        this.connections = connections;
        this.polling = polling();
    }

    private Notifications polling() {
        return new PollingNotifications(
                resources,
                List.of(
                        ResourceQuery.of(ResourceClass.NAMESPACE),
                        ResourceQuery.of(ResourceClass.DESTINATION),
                        ResourceQuery.of(ResourceClass.SUBSCRIPTION),
                        ResourceQuery.of(ResourceClass.ROUTE),
                        ResourceQuery.of(ResourceClass.POLICY),
                        ResourceQuery.of(ResourceClass.ACCESS_RULE),
                        ResourceQuery.of(ResourceClass.CONNECTION),
                        ResourceQuery.of(ResourceClass.CHANNEL)),
                RabbitMqOptions.watchInterval(cluster.spec()),
                cluster.options().clock(),
                BUFFER);
    }

    /** One subscriber's reader and what it must stop. */
    private final class Subscription implements AutoCloseable {
        private final AutoCloseable[] parts;
        private final @Nullable EventExchangeStream stream;

        Subscription(@Nullable EventExchangeStream stream, AutoCloseable... parts) {
            this.stream = stream;
            this.parts = parts;
        }

        @Override
        public void close() throws Exception {
            open.remove(this);
            Exception failure = null;
            for (AutoCloseable part : parts) {
                try {
                    part.close();
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    @Override
    public AutoCloseable subscribe(Scope scope, NotificationListener listener) {
        Optional<EventExchangeStream> stream = eventStream(listener);
        Subscription subscription;
        if (stream.isPresent()) {
            subscription = new Subscription(stream.get(), stream.get());
        } else {
            ConsumerWatch watch = new ConsumerWatch(cluster, listener);
            watch.start();
            AutoCloseable resourcesWatch = polling.subscribe(scope, listener);
            subscription = new Subscription(null, resourcesWatch, watch);
        }
        open.add(subscription);
        return subscription;
    }

    private Optional<EventExchangeStream> eventStream(NotificationListener listener) {
        if (connections.isEmpty()) {
            return Optional.empty();
        }
        try {
            if (!cluster.features().eventExchange()) {
                return Optional.empty();
            }
            EventExchangeStream stream = new EventExchangeStream(cluster, listener, connections.get());
            stream.start();
            return Optional.of(stream);
        } catch (BrokerException _) {
            return Optional.empty(); // A refusal or a failure to read falls back to polling, which retries.
        }
    }

    @Override
    public Map<NodeId, StreamState> state() {
        Set<StreamState> states = new HashSet<>();
        for (Subscription subscription : open) {
            if (subscription.stream != null) {
                states.add(subscription.stream.state());
            }
        }
        states.addAll(polling.state().values());
        return states.stream()
                .max(Comparator.comparing(state -> severity(state.phase())))
                .map(worst -> Map.of(RabbitMqCluster.CLUSTER, worst))
                .orElse(Map.of());
    }

    private static int severity(StreamState.Phase phase) {
        return switch (phase) {
            case ACTIVE -> 0;
            case CONNECTING -> 1;
            case BACKING_OFF -> 2;
            case UNAVAILABLE -> 3;
        };
    }

    /** Stops every subscription; a new interval, after a reconfiguration, applies to the ones made after. */
    void reset() {
        closeAll();
        polling = polling();
    }

    private void closeAll() {
        for (Subscription subscription : open) {
            try {
                subscription.close();
            } catch (Exception _) {
                // Closing a watch that is already stopped is not worth reporting.
                open.remove(subscription);
            }
        }
    }

    @Override
    public void close() {
        closeAll();
    }
}
