package io.github.sudoitir.openbroker.support;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NotificationDispatcherTest {

    private static final NodeId NODE = NodeId.of("n1");

    private static BrokerEvent event(String id) {
        return new BrokerEvent(id, NODE, "t", Optional.empty(), Instant.EPOCH, Attributes.empty());
    }

    @Test
    void aSlowListenerGetsAnOverflowGapInsteadOfStallingTheProducer() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        List<Object> seen = new CopyOnWriteArrayList<>();
        NotificationListener slow = new NotificationListener() {
            @Override
            public void onEvent(BrokerEvent e) {
                seen.add(e.id());
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onGap(Gap gap) {
                seen.add(gap.cause());
            }
        };
        try (NotificationDispatcher d = new NotificationDispatcher(slow, 3, Clock.systemUTC(), r -> r)) {
            d.event(event("first"));
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            for (int i = 0; i < 10; i++) {
                d.event(event("e" + i));
            }
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(1000);
            release.countDown();
            waitFor(() -> seen.contains(Gap.Cause.OVERFLOW));
        }
        assertThat(seen.getFirst()).isEqualTo("first");
        assertThat(seen).contains(Gap.Cause.OVERFLOW);
        assertThat(seen.size()).isLessThan(11);
    }

    @Test
    void deliversInOrderPerNode() {
        List<String> seen = new CopyOnWriteArrayList<>();
        NotificationListener l = new NotificationListener() {
            @Override
            public void onEvent(BrokerEvent e) {
                seen.add(e.id());
            }

            @Override
            public void onGap(Gap gap) {}
        };
        try (NotificationDispatcher d = new NotificationDispatcher(l, 1000, Clock.systemUTC(), r -> r)) {
            for (int i = 0; i < 100; i++) {
                d.event(event(String.valueOf(i)));
            }
            waitFor(() -> seen.size() == 100);
        }
        for (int i = 0; i < 100; i++) {
            assertThat(seen.get(i)).isEqualTo(String.valueOf(i));
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
