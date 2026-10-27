package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.operation.Operation;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The watch: Kafka sends no events, so the driver lists the cluster and reports the difference. */
class KafkaWatchIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka, Map.of(KafkaOptions.WATCH_INTERVAL, "PT1S"));
    }

    @AfterAll
    static void stop() {
        connection.close();
        drivers.close();
    }

    /** What a subscriber is told, in order. */
    private static final class Heard implements NotificationListener {
        final BlockingQueue<Object> heard = new LinkedBlockingQueue<>();
        final List<BrokerEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void onEvent(BrokerEvent event) {
            events.add(event);
            heard.add(event);
        }

        @Override
        public void onGap(Gap gap) {
            heard.add(gap);
        }

        /** The first event of this type about this subject, heard at any time so far or within the time allowed. */
        Optional<BrokerEvent> event(String type, ResourceId subject) throws InterruptedException {
            long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < until) {
                Optional<BrokerEvent> found = events.stream()
                        .filter(e -> e.type().equals(type)
                                && e.subject().filter(subject::equals).isPresent())
                        .findFirst();
                if (found.isPresent()) {
                    return found;
                }
                Thread.sleep(100);
            }
            return Optional.empty();
        }
    }

    @Test
    void aSubscriptionStartsWithAGapThenSeesTopicsAndSubscriptionsAppearAndGo() throws Exception {
        Heard heard = new Heard();
        try (AutoCloseable subscription =
                connection.notifications().orElseThrow().subscribe(Scope.whole(), heard)) {
            assertThat(heard.heard.poll(30, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(
                            Gap.class, g -> assertThat(g.cause()).isEqualTo(Gap.Cause.STARTED));

            String name = Kafka.unique("watched");
            ResourceId topic = new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
            ResourceId group = new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(Kafka.unique("g"), name));
            connection
                    .operations()
                    .execute(topic, new Operation.Create(Attributes.empty()), Scope.whole())
                    .orThrow();
            connection
                    .operations()
                    .execute(group, new Operation.Create(Attributes.empty()), Scope.whole())
                    .orThrow();

            assertThat(heard.event(EventTypes.RESOURCE_CREATED, topic)).isPresent();
            assertThat(heard.event(EventTypes.RESOURCE_CREATED, group)).isPresent();
            ResourceId partition = new ResourceId(KafkaKinds.PARTITION, ResourcePath.of(name, "0"));
            assertThat(heard.event(EventTypes.RESOURCE_CREATED, partition)).isPresent();

            connection
                    .operations()
                    .execute(topic, Operation.delete(), Scope.whole())
                    .orThrow();
            assertThat(heard.event(EventTypes.RESOURCE_DELETED, topic)).isPresent();
        }
    }

    @Test
    void everyStreamIsActiveOnceTheWatchHasRead() throws Exception {
        Heard heard = new Heard();
        try (AutoCloseable subscription =
                connection.notifications().orElseThrow().subscribe(Scope.whole(), heard)) {
            long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            Map<?, StreamState> states = Map.of();
            while (System.nanoTime() < until) {
                states = connection.notifications().orElseThrow().state();
                if (!states.isEmpty()
                        && states.values().stream().allMatch(s -> s.phase() == StreamState.Phase.ACTIVE)) {
                    break;
                }
                Thread.sleep(100);
            }
            assertThat(states).isNotEmpty();
            assertThat(states.values()).allSatisfy(s -> assertThat(s.phase()).isEqualTo(StreamState.Phase.ACTIVE));
        }
    }

    @Test
    void anUnreachableClusterBacksOffInsteadOfReportingNothing() throws Exception {
        ClusterSpec nowhere = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "127.0.0.1:1")),
                Map.of(KafkaOptions.WATCH_INTERVAL, "PT1S"));
        ConnectOptions quick =
                ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(1), Duration.ofSeconds(2)));
        try (ClusterConnection down = drivers.connect(
                        new ConnectRequest(KafkaDriver.SYSTEM, nowhere, CredentialSource.none(), quick));
                AutoCloseable subscription = down.notifications().orElseThrow().subscribe(Scope.whole(), new Heard())) {
            long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            Map<?, StreamState> states = Map.of();
            while (System.nanoTime() < until && states.isEmpty()) {
                states = down.notifications().orElseThrow().state();
                Thread.sleep(100);
            }
            assertThat(states.values()).allSatisfy(s -> {
                assertThat(s.phase()).isEqualTo(StreamState.Phase.BACKING_OFF);
                assertThat(s.lastFailure()).isPresent();
            });
        }
    }

    @Test
    void notificationsAreAvailableWhereTheAccountCanRead() {
        Assessment assessment = connection.access().probe().of(Capability.NOTIFICATIONS);

        assertThat(assessment.status()).isEqualTo(Assessment.Status.AVAILABLE);
        assertThat(assessment.reason()).contains("sends no events");
    }
}
