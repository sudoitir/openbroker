package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMq;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqTestCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The transport against a real three-node cluster: confirms, returns, settlement and the loss of a connection. */
class RabbitMqMessagingIT {

    private static final Duration WAIT = Duration.ofSeconds(60);

    private static RabbitMqCluster cluster;
    private static RabbitMqTransport transport;
    private static RabbitMq admin;
    private static final List<LinkEvent> EVENTS = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void connect() {
        RabbitMqTestCluster rabbit = RabbitMqTestCluster.shared();
        cluster = new RabbitMqCluster(
                rabbit.spec(), CredentialSource.of(rabbit.credentials()), new Context(ConnectOptions.defaults()));
        transport = new RabbitMqTransport(cluster);
        admin = RabbitMq.of(rabbit, 0);
    }

    @AfterAll
    static void close() {
        transport.close();
        cluster.close();
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

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static ResourceId queue(String name) {
        return ResourceId.of("rabbitmq.quorum-queue", "/", name);
    }

    private static OutboundMessage text(String body) {
        return new OutboundMessage(MessageHeaders.empty(), Map.of(), MessageBody.of(body.getBytes()), Map.of());
    }

    private static List<Message> receiveOne(Receiver receiver) {
        return RabbitMq.eventually(WAIT, () -> receiver.receive(1, Duration.ofSeconds(2)), found -> !found.isEmpty());
    }

    @Test
    void aSendIsConfirmedAndTheQueueHoldsIt() {
        String name = unique("send");
        admin.declareQueue("/", name, "quorum");
        try (Sender sender =
                transport.sender(SendTarget.subscription(queue(name)), Scope.whole(), SenderOptions.defaults())) {
            SendReceipt receipt = sender.send(text("one"));

            assertThat(receipt.resent()).isFalse();
            assertThat(RabbitMq.eventually(WAIT, () -> admin.messagesReady("/", name), n -> n == 1))
                    .isEqualTo(1);
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aSendToAnUnboundExchangeIsNotRouted() {
        String exchange = unique("unbound");
        admin.declareExchange("/", exchange, "direct");
        try (Sender sender = transport.sender(
                SendTarget.destination(ResourceId.of("rabbitmq.exchange", "/", exchange)),
                Scope.whole(),
                SenderOptions.defaults())) {
            assertThatThrownBy(() -> sender.send(text("lost"))).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
            });
        }
    }

    @Test
    void aMissingTargetIsNotFoundAtOpen() {
        assertThatThrownBy(() -> transport.sender(
                        SendTarget.subscription(queue(unique("missing"))), Scope.whole(), SenderOptions.defaults()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.NOT_FOUND));
    }

    @Test
    void aReceivedMessageIsAcknowledged() {
        String name = unique("ack");
        admin.declareQueue("/", name, "quorum");
        try (Receiver receiver = transport.receiver(queue(name), Scope.whole(), ReceiverOptions.consume())) {
            admin.publish("/", "", name, "hello");

            Message message = receiveOne(receiver).getFirst();
            assertThat(new String(message.body().bytes().orElseThrow().toByteArray()))
                    .isEqualTo("hello");
            receiver.settle(message, Disposition.accept());

            assertThat(RabbitMq.eventually(WAIT, () -> admin.messagesReady("/", name), n -> n == 0))
                    .isZero();
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aReleasedMessageIsDeliveredAgain() {
        String name = unique("requeue");
        admin.declareQueue("/", name, "quorum");
        try (Receiver receiver = transport.receiver(queue(name), Scope.whole(), ReceiverOptions.consume())) {
            admin.publish("/", "", name, "again");

            Message first = receiveOne(receiver).getFirst();
            receiver.settle(first, new Disposition.Release(Optional.empty(), true));
            Message second = receiveOne(receiver).getFirst();

            assertThat(second.extension("rabbitmq").raw("redelivered")).contains(true);
            receiver.settle(second, Disposition.accept());
        } finally {
            admin.deleteQueue("/", name);
        }
    }

    @Test
    void aClosedConnectionIsReportedAndTheLinkIsReopened() {
        String name = unique("loss");
        admin.declareQueue("/", name, "quorum");
        EVENTS.clear();
        try (AutoCloseable ignored = transport.watch(EVENTS::add);
                Receiver receiver = transport.receiver(queue(name), Scope.whole(), ReceiverOptions.consume())) {
            for (var connection : admin.get("/api/connections")) {
                if (connection.path("user_provided_name").asString().startsWith("openbroker")) {
                    admin.closeConnection(connection.path("name").asString());
                }
            }

            RabbitMq.eventually(
                    WAIT, () -> EVENTS.stream().anyMatch(e -> e.state() == LinkState.RECOVERING), seen -> seen);
            RabbitMq.eventually(WAIT, receiver::state, state -> state == LinkState.OPEN);
            admin.publish("/", "", name, "after");
            assertThat(receiveOne(receiver)).hasSize(1);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            admin.deleteQueue("/", name);
        }
    }
}
