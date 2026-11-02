package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConsumerWatchTest {

    private static final Map<String, String> JSON = Map.of("Content-Type", "application/json");
    private static final Instant NOW = Instant.parse("2026-03-04T05:06:07Z");

    private FakeManagement server;
    private ScheduledExecutorService timer;
    private ConsumerWatch watch;
    private final BlockingQueue<BrokerEvent> events = new LinkedBlockingQueue<>();

    @BeforeEach
    void start() {
        server = new FakeManagement();
        server.json("GET", "/api/queues/%2F", "queues.json");
        timer = mock(ScheduledExecutorService.class);
    }

    @AfterEach
    void stop() {
        if (watch != null) {
            watch.close();
        }
        server.close();
    }

    private ConsumerWatch watch() {
        RabbitMqCluster cluster = FakeManagement.cluster(
                Credentials.usernamePassword("ops", "s3cret"),
                ConnectOptions.defaults().withClock(Clock.fixed(NOW, ZoneOffset.UTC)),
                server.endpoint());
        NotificationListener listener = new NotificationListener() {
            @Override
            public void onEvent(BrokerEvent event) {
                events.add(event);
            }

            @Override
            public void onGap(Gap gap) {
                // Only the consumers matter here.
            }
        };
        watch = new ConsumerWatch(cluster, listener, timer);
        return watch;
    }

    private static String consumers(String... tags) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < tags.length; i++) {
            out.append(i == 0 ? "" : ",")
                    .append("{\"consumer_tag\":\"")
                    .append(tags[i])
                    .append("\",\"queue\":{\"name\":\"payments\",\"vhost\":\"/\"},")
                    .append("\"channel_details\":{\"name\":\"10.0.0.1:5555 -> 10.0.0.2:5672 (1)\"}}");
        }
        return out.append("]").toString();
    }

    private BrokerEvent next() throws InterruptedException {
        BrokerEvent event = events.poll(5, TimeUnit.SECONDS);
        assertThat(event).isNotNull();
        return event;
    }

    @Test
    void startsReadingAtTheWatchInterval() {
        server.respond("GET", "/api/consumers", 200, JSON, "[]");
        watch().start();
        verify(timer)
                .scheduleWithFixedDelay(
                        ArgumentCaptor.forClass(Runnable.class).capture(),
                        eq(10_000L),
                        eq(10_000L),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void reportsAConsumerThatAttachesAfterTheFirstReadAndOneThatLeaves() throws Exception {
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1"));
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1", "ctag-2"));
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-2"));
        ConsumerWatch w = watch();
        w.start();
        w.cycle();
        BrokerEvent attached = next();
        assertThat(attached.type()).isEqualTo(EventTypes.CONSUMER_ATTACHED);
        ResourceId queue = ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", "payments");
        assertThat(attached.subject()).contains(queue);
        assertThat(attached.data().get(StandardAttributes.SUBSCRIPTION)).contains(queue);
        assertThat(attached.data().get(StandardAttributes.CONSUMER))
                .contains(ResourceId.of(
                        RabbitMqKinds.CONSUMER.value(), "/", "10.0.0.1:5555 -> 10.0.0.2:5672 (1)", "ctag-2"));
        assertThat(attached.data().get(StandardAttributes.CHANNEL)).isPresent();
        assertThat(attached.time()).isEqualTo(NOW);
        assertThat(attached.source()).isEqualTo(RabbitMqCluster.CLUSTER);
        w.cycle();
        BrokerEvent detached = next();
        assertThat(detached.type()).isEqualTo(EventTypes.CONSUMER_DETACHED);
        assertThat(detached.data()
                        .get(StandardAttributes.CONSUMER)
                        .orElseThrow()
                        .path()
                        .segments())
                .endsWith("ctag-1");
        assertThat(events).isEmpty();
    }

    @Test
    void aConsumerPresentAtTheFirstReadIsNotNews() throws Exception {
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1"));
        ConsumerWatch w = watch();
        w.start();
        w.cycle();
        assertThat(events.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void aFailedCycleChangesNothingAndTheNextOneDiffsAgainstTheLastGoodRead() throws Exception {
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1"));
        server.respond("GET", "/api/consumers", 500, JSON, "{}");
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1", "ctag-2"));
        ConsumerWatch w = watch();
        w.start();
        w.cycle();
        assertThat(events.poll(200, TimeUnit.MILLISECONDS)).isNull();
        w.cycle();
        assertThat(next().type()).isEqualTo(EventTypes.CONSUMER_ATTACHED);
    }

    @Test
    void aBaselineThatCannotBeReadIsTakenByTheFirstCycleThatCan() throws Exception {
        server.respond("GET", "/api/consumers", 500, JSON, "{}");
        server.respond("GET", "/api/consumers", 500, JSON, "{}");
        server.respond("GET", "/api/consumers", 500, JSON, "{}");
        server.respond("GET", "/api/consumers", 200, JSON, consumers("ctag-1"));
        ConsumerWatch w = watch();
        w.start();
        w.cycle();
        assertThat(events.poll(200, TimeUnit.MILLISECONDS)).isNull();
        assertThat(server.received().stream().filter(r -> r.path().equals("/api/consumers")))
                .hasSize(4);
    }

    @Test
    void closeStopsTheTimer() {
        server.respond("GET", "/api/consumers", 200, JSON, "[]");
        ConsumerWatch w = watch();
        w.start();
        w.close();
        verify(timer).shutdownNow();
    }
}
