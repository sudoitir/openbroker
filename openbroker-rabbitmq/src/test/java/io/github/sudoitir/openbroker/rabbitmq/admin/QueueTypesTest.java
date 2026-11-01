package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueueTypesTest {

    /** A clock the test moves. */
    private static final class Moving extends Clock {
        private Instant now = Instant.parse("2026-11-01T10:00:00Z");

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    private FakeManagement server;
    private Moving clock;
    private RabbitMqCluster cluster;

    @BeforeEach
    void start() {
        server = new FakeManagement();
        server.json("GET", "/api/queues/%2F", "queues.json");
        clock = new Moving();
        cluster = new RabbitMqCluster(
                new ClusterSpec(List.of(), List.of(server.endpoint()), Map.of()),
                CredentialSource.of(Credentials.usernamePassword("ops", "pw")),
                new FakeManagement.TestContext(ConnectOptions.defaults().withClock(clock)));
    }

    @AfterEach
    void stop() {
        server.close();
    }

    @Test
    void namesAQueuesKindFromTheReducedListing() {
        QueueTypes types = cluster.queueTypes();
        assertThat(types.kindOf("/", "orders")).isEqualTo(RabbitMqKinds.CLASSIC_QUEUE);
        assertThat(types.kindOf("/", "payments")).isEqualTo(RabbitMqKinds.QUORUM_QUEUE);
        assertThat(types.kindOf("/", "audit")).isEqualTo(RabbitMqKinds.STREAM);
        FakeManagement.Received request = server.received().getFirst();
        assertThat(request.query()).contains("columns=name%2Cvhost%2Ctype").contains("disable_stats=true");
    }

    @Test
    void keepsTheListingForTheWatchIntervalThenReadsAgain() {
        QueueTypes types = cluster.queueTypes();
        types.kindOf("/", "orders");
        types.kindOf("/", "payments");
        assertThat(server.received()).hasSize(1);
        clock.advance(Duration.ofSeconds(11));
        types.kindOf("/", "orders");
        assertThat(server.received()).hasSize(2);
    }

    @Test
    void anUnknownQueueIsLookedForOnceMoreThenTakenForClassic() {
        QueueTypes types = cluster.queueTypes();
        types.kindOf("/", "orders");
        assertThat(types.kindOf("/", "new"))
                .as("within half a second: no second read")
                .isEqualTo(RabbitMqKinds.CLASSIC_QUEUE);
        assertThat(server.received()).hasSize(1);
        clock.advance(Duration.ofSeconds(2));
        assertThat(types.kindOf("/", "mqtt")).isEqualTo(RabbitMqKinds.CLASSIC_QUEUE);
        assertThat(server.received()).hasSize(2);
    }

    @Test
    void resetForgetsEverything() {
        QueueTypes types = cluster.queueTypes();
        types.kindOf("/", "orders");
        types.reset();
        types.kindOf("/", "orders");
        assertThat(server.received()).hasSize(2);
    }

    @Test
    void readsEveryPageOfALongListing() {
        try (FakeManagement paged = new FakeManagement()) {
            paged.respond(
                            "GET",
                            "/api/queues/v",
                            200,
                            Map.of(),
                            "{\"items\":[{\"name\":\"a\",\"type\":\"quorum\"}],\"page\":1,\"page_count\":2}")
                    .respond(
                            "GET",
                            "/api/queues/v",
                            200,
                            Map.of(),
                            "{\"items\":[{\"name\":\"b\",\"type\":\"stream\"}],\"page\":2,\"page_count\":2}");
            QueueTypes types = FakeManagement.cluster(paged.endpoint()).queueTypes();
            assertThat(types.kindOf("v", "b")).isEqualTo(RabbitMqKinds.STREAM);
            assertThat(types.kindOf("v", "a")).isEqualTo(RabbitMqKinds.QUORUM_QUEUE);
            assertThat(paged.received()).hasSize(2);
        }
    }

    @Test
    void acceptsAPlainArrayToo() {
        try (FakeManagement plain = new FakeManagement()) {
            plain.respond("GET", "/api/queues/v", 200, Map.of(), "[{\"name\":\"a\",\"type\":\"stream\"}]");
            assertThat(FakeManagement.cluster(plain.endpoint()).queueTypes().kindOf("v", "a"))
                    .isEqualTo(RabbitMqKinds.STREAM);
        }
    }
}
