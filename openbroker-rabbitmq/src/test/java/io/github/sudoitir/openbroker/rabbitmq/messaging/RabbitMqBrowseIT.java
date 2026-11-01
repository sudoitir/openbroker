package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMq;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTestCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Browsing and counting against a real cluster: a stream is read without taking anything, a queue is refused. */
class RabbitMqBrowseIT {

    private static final Duration SETTLE = Duration.ofSeconds(30);

    private static RabbitMqCluster cluster;
    private static RabbitMqMessages messages;
    private static RabbitMq admin;

    @BeforeAll
    static void connect() {
        RabbitMqTestCluster rabbit = RabbitMqTestCluster.shared();
        cluster = new RabbitMqCluster(
                rabbit.spec(), CredentialSource.of(rabbit.credentials()), new Context(ConnectOptions.defaults()));
        messages = new RabbitMqMessages(cluster);
        admin = RabbitMq.of(rabbit, 0);
    }

    @AfterAll
    static void close() {
        messages.close();
        cluster.close();
    }

    private static BrowseRequest request(Position position, int limit) {
        return new BrowseRequest(position, limit, Optional.empty(), 1024);
    }

    private static void send(ResourceId target, int count) {
        for (int i = 0; i < count; i++) {
            messages.send(
                    SendTarget.subscription(target),
                    Scope.whole(),
                    OutboundMessage.of(MessageBody.of(("m" + i).getBytes())));
        }
    }

    @Test
    void aStreamIsBrowsedInPagesAndKeepsItsMessages() {
        String name = "browse-" + UUID.randomUUID();
        admin.declareQueue("/", name, "stream");
        ResourceId stream = ResourceId.of("rabbitmq.stream", "/", name);
        try {
            send(stream, 5);
            BrowsePage first = RabbitMq.eventually(
                    SETTLE,
                    () -> messages.browse(stream, Scope.whole(), request(Position.earliest(), 3)),
                    page -> page.messages().size() == 3);
            BrowsePage second = messages.browse(
                    stream, Scope.whole(), request(Position.after(first.next().orElseThrow()), 3));
            BrowsePage again = messages.browse(stream, Scope.whole(), request(Position.earliest(), 10));

            assertThat(first.messages()).extracting(m -> m.ref().position()).containsExactly("0", "1", "2");
            assertThat(second.messages()).extracting(m -> m.ref().position()).containsExactly("3", "4");
            assertThat(again.messages()).hasSize(5);
            assertThat(again.next()).isEmpty();
            assertThat(RabbitMq.eventually(
                            SETTLE,
                            () -> messages.count(stream, Scope.whole(), Optional.empty())
                                    .value()
                                    .count(),
                            count -> count instanceof Count.Estimate e && e.value() == 5))
                    .isEqualTo(Count.estimate(5));
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aQueueIsRefusedAndKeepsItsMessages() {
        String name = "browse-" + UUID.randomUUID();
        admin.declareQueue("/", name, "classic");
        ResourceId queue = ResourceId.of("rabbitmq.classic-queue", "/", name);
        try {
            send(queue, 2);
            assertThatThrownBy(() -> messages.browse(queue, Scope.whole(), request(Position.earliest(), 5)))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                        assertThat(e.info().reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
                    });
            assertThat(RabbitMq.eventually(SETTLE, () -> admin.messagesReady("/", name), ready -> ready == 2))
                    .isEqualTo(2);
            assertThat(messages.count(queue, Scope.whole(), Optional.empty())
                            .value()
                            .count())
                    .isEqualTo(Count.exact(2));
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    private record Context(ConnectOptions options) implements DriverContext {
        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return factory.get();
        }

        @Override
        public void evict(String key) {
            // Nothing is kept.
        }
    }
}
