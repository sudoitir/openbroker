package io.github.sudoitir.openbroker.support;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.StreamState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PollingNotificationsTest {

    private static final NodeId NODE = NodeId.of("n1");

    /** A cluster whose listing the test controls. */
    private static final class Listing implements Resources {
        final List<ResourceId> present = new CopyOnWriteArrayList<>();
        volatile BrokerException failure;

        @Override
        public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
            if (failure != null) {
                return new Partial<>(
                        List.of(new ResourcePage(List.of(), Optional.empty())),
                        Map.of(NODE, NodeOutcome.FAILED),
                        Map.of(NODE, failure));
            }
            List<Resource> items = present.stream()
                    .map(id -> new Resource(
                            id,
                            Attributes.empty(),
                            List.of(),
                            List.of(Placement.instance(NODE, Attributes.empty())),
                            Instant.now()))
                    .toList();
            return Partial.complete(List.of(new ResourcePage(items, Optional.empty())), Set.of(NODE));
        }

        @Override
        public List<Kind> kinds() {
            return List.of();
        }
    }

    private static final class Heard implements NotificationListener {
        final BlockingQueue<Object> heard = new LinkedBlockingQueue<>();

        @Override
        public void onEvent(BrokerEvent event) {
            heard.add(event);
        }

        @Override
        public void onGap(Gap gap) {
            heard.add(gap);
        }

        Object next() throws InterruptedException {
            return heard.poll(5, TimeUnit.SECONDS);
        }
    }

    private static PollingNotifications watching(Listing listing) {
        return new PollingNotifications(
                listing,
                List.of(ResourceQuery.of(ResourceClass.DESTINATION)),
                Duration.ofMillis(20),
                Clock.systemUTC(),
                100);
    }

    @Test
    void aNodeWithNothingOnItStillStartsItsStream() throws Exception {
        Listing listing = new Listing();
        Heard heard = new Heard();
        ResourceId made = ResourceId.of("test.thing", "made");
        PollingNotifications watch = watching(listing);

        try (AutoCloseable subscription = watch.subscribe(Scope.whole(), heard)) {
            assertThat(heard.next()).isInstanceOfSatisfying(Gap.class, g -> {
                assertThat(g.node()).isEqualTo(NODE);
                assertThat(g.cause()).isEqualTo(Gap.Cause.STARTED);
            });
            listing.present.add(made);
            assertThat(heard.next()).isInstanceOfSatisfying(BrokerEvent.class, e -> {
                assertThat(e.type()).isEqualTo(EventTypes.RESOURCE_CREATED);
                assertThat(e.subject()).contains(made);
            });
            listing.present.remove(made);
            assertThat(heard.next())
                    .isInstanceOfSatisfying(
                            BrokerEvent.class, e -> assertThat(e.type()).isEqualTo(EventTypes.RESOURCE_DELETED));
        }
        assertThat(watch.state().get(NODE).phase()).isEqualTo(StreamState.Phase.ACTIVE);
    }

    @Test
    void aNodeThatFailsBacksOffAndIsToldAgainWhenItReturns() throws Exception {
        Listing listing = new Listing();
        Heard heard = new Heard();
        PollingNotifications watch = watching(listing);

        try (AutoCloseable subscription = watch.subscribe(Scope.whole(), heard)) {
            assertThat(heard.next()).isInstanceOf(Gap.class);
            listing.failure = new BrokerException(
                    Status.UNAVAILABLE, ErrorInfo.of("UNREACHABLE"), Dispatch.IN_DOUBT, NODE, "down");
            assertThat(heard.next())
                    .isInstanceOfSatisfying(
                            Gap.class, g -> assertThat(g.cause()).isEqualTo(Gap.Cause.RECONNECTED));
            assertThat(watch.state().get(NODE).phase()).isEqualTo(StreamState.Phase.BACKING_OFF);
            listing.failure = null;
            assertThat(heard.next())
                    .isInstanceOfSatisfying(
                            Gap.class, g -> assertThat(g.cause()).isEqualTo(Gap.Cause.STARTED));
        }
    }
}
