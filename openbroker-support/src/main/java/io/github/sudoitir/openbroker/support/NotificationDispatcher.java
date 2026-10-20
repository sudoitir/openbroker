package io.github.sudoitir.openbroker.support;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.UnaryOperator;

/**
 * Delivers notifications to one listener with the SDK's contract: serially per node, off the thread
 * that produced them, through a bounded buffer per node. When a node's buffer is full its buffered
 * notifications are dropped and the listener receives a {@link Gap} with cause {@code OVERFLOW}, so a
 * slow listener never stalls the broker connection and never silently misses a notification.
 *
 * <p>Thread-safe. {@link #close()} stops delivery.
 */
public final class NotificationDispatcher implements AutoCloseable {

    private sealed interface Item {}

    private record Event(BrokerEvent event) implements Item {}

    private record GapItem(Gap gap) implements Item {}

    private final NotificationListener listener;
    private final int capacity;
    private final Clock clock;
    private final UnaryOperator<Runnable> propagation;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<NodeId, Lane> lanes = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public NotificationDispatcher(
            NotificationListener listener, int capacity, Clock clock, UnaryOperator<Runnable> propagation) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.listener = listener;
        this.capacity = capacity;
        this.clock = clock;
        this.propagation = propagation;
    }

    public void event(BrokerEvent event) {
        lane(event.source()).offer(new Event(event));
    }

    public void gap(Gap gap) {
        lane(gap.node()).offer(new GapItem(gap));
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();
    }

    private Lane lane(NodeId node) {
        return lanes.computeIfAbsent(node, Lane::new);
    }

    private final class Lane {
        private final NodeId node;
        private final Deque<Item> queue = new ArrayDeque<>();
        private boolean draining;

        Lane(NodeId node) {
            this.node = node;
        }

        void offer(Item item) {
            if (closed) {
                return;
            }
            boolean start;
            synchronized (this) {
                if (queue.size() >= capacity) {
                    queue.clear();
                    queue.add(new GapItem(new Gap(node, clock.instant(), Gap.Cause.OVERFLOW)));
                } else {
                    queue.add(item);
                }
                start = !draining;
                draining = true;
            }
            if (start) {
                executor.execute(propagation.apply(this::drain));
            }
        }

        private void drain() {
            while (!closed) {
                Item next;
                synchronized (this) {
                    next = queue.poll();
                    if (next == null) {
                        draining = false;
                        return;
                    }
                }
                try {
                    switch (next) {
                        case Event e -> listener.onEvent(e.event());
                        case GapItem g -> listener.onGap(g.gap());
                    }
                } catch (RuntimeException e) {
                    System.getLogger(NotificationDispatcher.class.getName())
                            .log(System.Logger.Level.WARNING, "A notification listener failed", e);
                }
            }
        }
    }
}
