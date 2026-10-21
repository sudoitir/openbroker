package io.github.sudoitir.openbroker.artemis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.Operation;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Links that lose their connection to a broker that stays up: the broker closes the client's
 * connections, and the driver reopens each link on the same node. What happens to messages in the
 * air is the contract's, and is checked here against a real broker; failover to a backup and a
 * broker that dies are the chaos TCK's.
 */
@Testcontainers
class ArtemisRecoveryIT {

    @Container
    static final GenericContainer<?> BROKER = TestBroker.container();

    private static final String CLOSE = "closeConnectionsForUser(java.lang.String)";

    private BrokerDrivers drivers;
    private ClusterConnection admin;
    private ClusterConnection client;
    private Transport transport;
    private final List<ResourceId> queues = new ArrayList<>();

    @BeforeEach
    void connect() {
        drivers = BrokerDrivers.of(List.of(new ArtemisDriver()));
        admin = drivers.connect(TestBroker.request(BROKER, TestBroker.USER, TestBroker.PASSWORD));
        client =
                drivers.connect(TestBroker.request(BROKER, TestBroker.RESTRICTED_USER, TestBroker.RESTRICTED_PASSWORD));
        transport = client.transport().orElseThrow();
    }

    @AfterEach
    void disconnect() {
        client.close();
        for (ResourceId queue : queues) {
            admin.operations().execute(queue, new Operation.Delete(false, true), Scope.whole());
        }
        admin.close();
        drivers.close();
    }

    /** An idempotent sender that loses its link twice mid-stream delivers every message once, in the end. */
    @Test
    void anIdempotentSenderThatLosesItsLinkDeliversEveryMessageOnce() throws Exception {
        ResourceId queue = newQueue();
        List<LinkEvent> events = new CopyOnWriteArrayList<>();
        int count = 300;
        try (AutoCloseable watch = transport.watch(events::add);
                Sender sender = transport.sender(
                        SendTarget.subscription(queue),
                        Scope.whole(),
                        SenderOptions.defaults().withIdempotent(true))) {
            List<CompletionStage<SendReceipt>> receipts = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (i == 100 || i == 200) {
                    closeClientConnections();
                }
                receipts.add(((Offer.Accepted) sender.offer(message(i))).receipt());
            }
            for (CompletionStage<SendReceipt> receipt : receipts) {
                receipt.toCompletableFuture().get(60, TimeUnit.SECONDS);
            }
            assertThat(sender.state()).isEqualTo(LinkState.OPEN);
        }
        assertThat(events).extracting(LinkEvent::state).contains(LinkState.RECOVERING, LinkState.OPEN);
        try (Receiver receiver = transport.receiver(queue, Scope.whole(), ReceiverOptions.consume())) {
            List<Long> seqs = new ArrayList<>();
            await("every message", () -> {
                for (Message m : receiver.receive(100, Duration.ofSeconds(1))) {
                    seqs.add(seq(m));
                    receiver.settle(m, Disposition.accept());
                }
                return seqs.size() >= count;
            });
            assertThat(seqs).as("no loss and no duplicate").hasSize(count);
            assertThat(new TreeSet<>(seqs)).hasSize(count);
        }
    }

    /** A message handed out before the link was lost is settled as stale, and the broker delivers it again. */
    @Test
    void aMessageFromBeforeALostLinkIsStaleAndDeliveredAgain() {
        ResourceId queue = newQueue();
        admin.messages().send(SendTarget.subscription(queue), message(1));
        List<LinkEvent> events = new CopyOnWriteArrayList<>();
        try (AutoCloseable watch = transport.watch(events::add);
                Receiver receiver = transport.receiver(queue, Scope.whole(), ReceiverOptions.consume())) {
            Message first = receiveOne(receiver);
            closeClientConnections();
            await("the link recovered", () -> events.stream().anyMatch(e -> e.state() == LinkState.OPEN));

            assertThatThrownBy(() -> receiver.settle(first, Disposition.accept()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.DELIVERY_STALE);
                        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                    });
            Message again = receiveOne(receiver);
            assertThat(seq(again)).isEqualTo(1);
            receiver.settle(again, Disposition.accept());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** A processor whose link is lost carries on, and the messages in the air are handled again, not lost. */
    @Test
    void aProcessorCarriesOnAfterALostLink() throws Exception {
        ResourceId queue = newQueue();
        int count = 30;
        for (int i = 0; i < count; i++) {
            admin.messages().send(SendTarget.subscription(queue), message(i));
        }
        Set<Long> handled = ConcurrentHashMap.newKeySet();
        try (Processor processor = transport.process(
                queue, Scope.whole(), ProcessorOptions.defaults().withConcurrency(3), ctx -> {
                    Thread.sleep(20);
                    handled.add(seq(ctx.message()));
                    return Disposition.accept();
                })) {
            await("some handled", () -> handled.size() >= 5);
            closeClientConnections();
            await("every message handled", () -> handled.size() == count);
            assertThat(processor.state()).isIn(LinkState.OPEN, LinkState.RECOVERING);
        }
    }

    private void closeClientConnections() {
        admin.unwrap(ArtemisManagement.class)
                .orElseThrow()
                .jolokia(NodeId.of("primary"))
                .execOnBroker(CLOSE, TestBroker.RESTRICTED_USER);
    }

    private ResourceId newQueue() {
        ResourceId id = new ResourceId(
                ArtemisKinds.QUEUE,
                ResourcePath.of("tck.recovery." + UUID.randomUUID().toString().substring(0, 8)));
        admin.operations()
                .execute(
                        id,
                        new Operation.Create(Attributes.builder()
                                .put(StandardAttributes.DURABLE, true)
                                .build()),
                        Scope.whole())
                .orThrow();
        queues.add(id);
        return id;
    }

    private static Message receiveOne(Receiver receiver) {
        List<Message> got = new ArrayList<>();
        await("a message", () -> got.addAll(receiver.receive(1, Duration.ofSeconds(1))) && !got.isEmpty());
        return got.getFirst();
    }

    private static OutboundMessage message(long seq) {
        return OutboundMessage.of(MessageBody.of(("m" + seq).getBytes(StandardCharsets.UTF_8)))
                .withProperties(Map.of("seq", PropertyValue.of(seq)));
    }

    private static long seq(Message message) {
        return ((Number) message.properties().get("seq").boxed()).longValue();
    }

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < until) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (BrokerException e) {
                // a link that is recovering refuses; the next try is after it
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for " + what, e);
            }
        }
        throw new AssertionError("Timed out waiting for " + what);
    }
}
