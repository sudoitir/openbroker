package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.support.PollingNotifications;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka sends no events, so the watch reads the cluster every {@code watch.interval} and reports
 * what appeared and disappeared: topics, partitions (per broker that holds a replica), groups and
 * subscriptions. A read that fails is followed by a gap for the nodes it covered. The stream's
 * events are creations and deletions, and consumers attaching to and leaving a subscription.
 */
public final class KafkaNotifications implements Notifications, AutoCloseable {

    /** How many events a subscriber may fall behind by before it is told it missed some. */
    private static final int BUFFER = 1024;

    private final KafkaCluster cluster;
    private final KafkaResources resources;
    private final Handlers handlers;
    private final Set<AutoCloseable> open = ConcurrentHashMap.newKeySet();
    private volatile Notifications polling;

    KafkaNotifications(KafkaCluster cluster, KafkaResources resources, Handlers handlers) {
        this.cluster = cluster;
        this.resources = resources;
        this.handlers = handlers;
        this.polling = polling();
    }

    private Notifications polling() {
        return new PollingNotifications(
                resources.ids(),
                List.of(
                        ResourceQuery.of(ResourceClass.DESTINATION),
                        ResourceQuery.of(ResourceClass.PARTITION),
                        ResourceQuery.of(ResourceClass.CONSUMER_GROUP),
                        ResourceQuery.of(ResourceClass.SUBSCRIPTION)),
                cluster.watchInterval(),
                cluster.options().clock(),
                BUFFER);
    }

    @Override
    public AutoCloseable subscribe(Scope scope, NotificationListener listener) {
        AutoCloseable subscription = polling.subscribe(scope, listener);
        MemberWatch members = new MemberWatch(cluster, handlers, listener);
        AutoCloseable tracked = new AutoCloseable() {
            @Override
            public void close() throws Exception {
                open.remove(this);
                members.close();
                subscription.close();
            }
        };
        open.add(tracked);
        return tracked;
    }

    @Override
    public Map<NodeId, StreamState> state() {
        return polling.state();
    }

    /** Stops every subscription; a new interval, after a reconfiguration, applies to the ones made after. */
    void reset() {
        closeAll();
        polling = polling();
    }

    private void closeAll() {
        for (AutoCloseable subscription : open) {
            try {
                subscription.close();
            } catch (Exception e) {
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
