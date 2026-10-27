package io.github.sudoitir.openbroker.support;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.notification.StreamState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Notifications for a system without push notifications, built by listing resources periodically
 * and reporting what appeared and disappeared, per node, the way a Kubernetes informer turns lists
 * into a watch. A cycle that fails on a node is followed by a {@link Gap} for that node.
 */
public final class PollingNotifications implements Notifications {

    private final Resources resources;
    private final List<ResourceQuery> queries;
    private final Duration interval;
    private final Clock clock;
    private final int buffer;
    private final Map<NodeId, StreamState> states = new ConcurrentHashMap<>();

    /**
     * @param queries what to list each cycle (paging is followed to the end)
     * @param buffer the per-node notification buffer before an overflow gap
     */
    public PollingNotifications(
            Resources resources, List<ResourceQuery> queries, Duration interval, Clock clock, int buffer) {
        this.resources = resources;
        this.queries = List.copyOf(queries);
        this.interval = interval;
        this.clock = clock;
        this.buffer = buffer;
    }

    @Override
    public AutoCloseable subscribe(Scope scope, NotificationListener listener) {
        NotificationDispatcher dispatcher = new NotificationDispatcher(listener, buffer, clock, r -> r);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("openbroker-poll").factory());
        Poller poller = new Poller(scope, dispatcher);
        timer.scheduleWithFixedDelay(poller::cycle, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        return () -> {
            timer.shutdownNow();
            dispatcher.close();
        };
    }

    @Override
    public Map<NodeId, StreamState> state() {
        return Map.copyOf(states);
    }

    private final class Poller {
        private final Scope scope;
        private final NotificationDispatcher dispatcher;
        private final Map<NodeId, Set<ResourceId>> known = new HashMap<>();

        Poller(Scope scope, NotificationDispatcher dispatcher) {
            this.scope = scope;
            this.dispatcher = dispatcher;
        }

        void cycle() {
            Instant now = clock.instant();
            Map<NodeId, Set<ResourceId>> seen = new HashMap<>();
            Map<NodeId, BrokerException> failed = new HashMap<>();
            for (ResourceQuery q : queries) {
                ResourceQuery query = q.in(scope);
                while (true) {
                    Partial<ResourcePage> page = resources.read(query);
                    failed.putAll(page.failures());
                    // A node that answered and has nothing is still watched: the stream starts, and what
                    // appears on it later is news.
                    page.outcomes().forEach((node, outcome) -> {
                        if (outcome == NodeOutcome.APPLIED) {
                            seen.computeIfAbsent(node, n -> new HashSet<>());
                        }
                    });
                    for (Resource r : page.value().items()) {
                        for (Placement p : r.placements()) {
                            seen.computeIfAbsent(p.node(), n -> new HashSet<>()).add(r.id());
                        }
                    }
                    Optional<String> next = page.value().continueToken();
                    if (next.isEmpty()) {
                        break;
                    }
                    query = query.continueFrom(next.get());
                }
            }
            failed.forEach((node, error) -> {
                // The state is published first, so a listener told of the gap finds the node backing off.
                states.put(node, new StreamState(StreamState.Phase.BACKING_OFF, Optional.of(now), Optional.of(error)));
                if (known.remove(node) != null) {
                    dispatcher.gap(new Gap(node, now, Gap.Cause.RECONNECTED));
                }
            });
            seen.forEach((node, ids) -> {
                if (failed.containsKey(node)) {
                    return;
                }
                Set<ResourceId> before = known.get(node);
                if (before == null) {
                    dispatcher.gap(new Gap(node, now, Gap.Cause.STARTED));
                } else {
                    ids.stream()
                            .filter(id -> !before.contains(id))
                            .forEach(id -> emit(node, EventTypes.RESOURCE_CREATED, id, now));
                    before.stream()
                            .filter(id -> !ids.contains(id))
                            .forEach(id -> emit(node, EventTypes.RESOURCE_DELETED, id, now));
                }
                known.put(node, ids);
                states.put(node, new StreamState(StreamState.Phase.ACTIVE, Optional.of(now), Optional.empty()));
            });
        }

        private void emit(NodeId node, String type, ResourceId id, Instant at) {
            dispatcher.event(
                    new BrokerEvent(UUID.randomUUID().toString(), node, type, Optional.of(id), at, Attributes.empty()));
        }
    }
}
