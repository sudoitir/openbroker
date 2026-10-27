package io.github.sudoitir.openbroker.tck;

import static io.github.sudoitir.openbroker.tck.TckSupport.await;
import static io.github.sudoitir.openbroker.tck.TckSupport.confirm;
import static io.github.sudoitir.openbroker.tck.TckSupport.drain;
import static io.github.sudoitir.openbroker.tck.TckSupport.message;
import static io.github.sudoitir.openbroker.tck.TckSupport.offerAll;
import static io.github.sudoitir.openbroker.tck.TckSupport.pause;
import static io.github.sudoitir.openbroker.tck.TckSupport.receiveN;
import static io.github.sudoitir.openbroker.tck.TckSupport.sendTolerating;
import static io.github.sudoitir.openbroker.tck.TckSupport.seq;
import static io.github.sudoitir.openbroker.tck.TckSupport.seqs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The messaging conformance kit: the delivery guarantees, idempotence, ordering, flow control,
 * retries, joined transactions and security of the transport, against a real broker. Extend it in a
 * driver's integration test and return a {@link TckEnvironment} backed by one.
 *
 * <p>A case whose guarantee, ordering or feature the transport's {@link TransportCapabilities}
 * declare unavailable is skipped, not failed. Every case makes its own subscriptions, with unique
 * names, and removes them and closes what it opened. The cases that need a lost link, a killed
 * broker or a failover are in {@link ChaosTck}.
 */
public abstract class MessagingTck {

    private static final Duration SECONDS_5 = Duration.ofSeconds(5);
    private static final Duration SECONDS_15 = Duration.ofSeconds(15);
    private static final Duration SECONDS_60 = Duration.ofSeconds(60);
    private static final Duration QUIET = Duration.ofSeconds(1);

    private TckEnvironment env;
    private BrokerDrivers drivers;
    private ClusterConnection connection;
    private Transport transport;
    private TransportCapabilities caps;
    private final List<ResourceId> created = new ArrayList<>();
    private final Deque<AutoCloseable> open = new ArrayDeque<>();

    /** The environment; called once per test. */
    protected abstract TckEnvironment environment();

    @BeforeEach
    void connect() {
        env = environment();
        drivers = BrokerDrivers.of(List.of(env.driver()));
        connection = drivers.connect(env.request());
        transport = connection.transport().orElse(null);
        assumeTrue(transport != null, "The driver offers no transport");
        caps = transport.capabilities();
    }

    @AfterEach
    void disconnect() {
        while (!open.isEmpty()) {
            try {
                open.pop().close();
            } catch (Exception ignored) {
                // Best effort: a case may have closed it already.
            }
        }
        if (connection != null) {
            for (ResourceId id : created) {
                try {
                    connection.operations().execute(id, new Operation.Delete(false, true), Scope.whole());
                } catch (BrokerException ignored) {
                    // Best effort: a case may have deleted it.
                }
            }
            connection.close();
        }
        if (drivers != null) {
            drivers.close();
        }
    }

    // Guarantees.

    /**
     * An at-most-once receiver never gets back a message it was handed: closing it without settling
     * leaves nothing for anyone else.
     */
    @Test
    void atMostOnceNeverRedelivers() {
        assumeGuarantee(Guarantee.AT_MOST_ONCE);
        ResourceId id = newSubscription("amo");
        sendDirect(id, 5);
        try (Receiver receiver = transport.receiver(
                id, Scope.whole(), ReceiverOptions.consume().withGuarantee(Guarantee.AT_MOST_ONCE))) {
            assertThat(receiveN(receiver, 5, SECONDS_15)).hasSize(5);
        }
        assertThat(drain(transport, id, Duration.ofSeconds(2))).isEmpty();
    }

    /**
     * An at-least-once receiver that closes without settling leaves its messages to be delivered
     * again, each once more, and none is lost.
     */
    @Test
    void atLeastOnceRedeliversUnsettledAfterClose() {
        assumeGuarantee(Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription("alo");
        sendDirect(id, 3);
        try (Receiver receiver = transport.receiver(id, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiveN(receiver, 3, SECONDS_15)).hasSize(3);
        }
        List<Message> again = drain(transport, id, 3, SECONDS_15, QUIET);
        assertThat(seqs(again)).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    /**
     * A transacted receiver's settlements take effect only on commit: after a rollback the messages
     * are delivered again, and after a commit they are gone.
     */
    @Test
    void transactedRollbackRedeliversAndCommitRemoves() {
        assumeGuarantee(Guarantee.TRANSACTED);
        ResourceId id = newSubscription("tx");
        sendDirect(id, 3);
        try (Receiver receiver =
                transport.receiver(id, Scope.whole(), ReceiverOptions.consume().withGuarantee(Guarantee.TRANSACTED))) {
            List<Message> first = receiveN(receiver, 3, SECONDS_15);
            assertThat(first).hasSize(3);
            first.forEach(m -> receiver.settle(m, Disposition.accept()));
            receiver.rollback();

            List<Message> again = receiveN(receiver, 3, SECONDS_15);
            assertThat(seqs(again)).containsExactlyInAnyOrder(0L, 1L, 2L);
            again.forEach(m -> receiver.settle(m, Disposition.accept()));
            receiver.commit();
        }
        assertThat(drain(transport, id, Duration.ofSeconds(2))).isEmpty();
    }

    /**
     * One transaction can receive and send: a rollback leaves the input where it was and delivers
     * nothing, and one commit moves every message, each exactly once.
     */
    @Test
    void joinedTransactionMovesExactlyOnce() {
        assumeGuarantee(Guarantee.TRANSACTED);
        assumeTrue(subscription().joinedTransactions(), "The transport declares no joined transactions");
        ResourceId from = newSubscription("join-from");
        ResourceId to = newSubscription("join-to");
        sendDirect(from, 5);
        try (Receiver receiver = transport.receiver(
                from, Scope.whole(), ReceiverOptions.consume().withGuarantee(Guarantee.TRANSACTED))) {
            Sender joined = receiver.sender(SendTarget.subscription(to));
            for (int attempt = 1; attempt <= 2; attempt++) {
                List<Message> received = receiveN(receiver, 5, SECONDS_15);
                assertThat(received).hasSize(5);
                for (Message m : received) {
                    joined.send(OutboundMessage.copyOf(m));
                    receiver.settle(m, Disposition.accept());
                }
                if (attempt == 1) {
                    receiver.rollback();
                    assertThat(exactCount(to)).as("a rollback delivers nothing").isZero();
                    assertThat(exactCount(from))
                            .as("a rollback keeps the input")
                            .isEqualTo(5);
                } else {
                    receiver.commit();
                }
            }
        }
        assertThat(drain(transport, from, Duration.ofSeconds(2))).isEmpty();
        assertThat(seqs(drain(transport, to, 5, SECONDS_15, QUIET))).containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L);
    }

    /**
     * A guarantee, ordering or deduplication scope the transport does not declare is refused when the link
     * is opened, as unimplemented and not sent, never accepted and then not kept.
     */
    @Test
    void undeclaredGuaranteeOrOrderingIsUnimplemented() {
        List<Guarantee> missingGuarantees = new ArrayList<>(List.of(Guarantee.values()));
        missingGuarantees.removeAll(caps.guarantees());
        List<OrderingScope> missingOrderings = new ArrayList<>(List.of(OrderingScope.values()));
        missingOrderings.removeAll(subscription().orderings());
        List<DeduplicationScope> missingScopes = new ArrayList<>(List.of(DeduplicationScope.values()));
        missingScopes.removeIf(caps.deduplication()::satisfies);
        assumeTrue(
                !missingGuarantees.isEmpty() || !missingOrderings.isEmpty() || !missingScopes.isEmpty(),
                "The transport declares everything");
        ResourceId id = newSubscription("undeclared");
        for (Guarantee guarantee : missingGuarantees) {
            assertUnimplemented(() -> transport
                    .sender(
                            SendTarget.subscription(id),
                            Scope.whole(),
                            SenderOptions.defaults().withGuarantee(guarantee))
                    .close());
            assertUnimplemented(() -> transport
                    .receiver(id, Scope.whole(), ReceiverOptions.consume().withGuarantee(guarantee))
                    .close());
        }
        for (OrderingScope ordering : missingOrderings) {
            assertUnimplemented(() -> transport
                    .process(
                            id,
                            Scope.whole(),
                            ProcessorOptions.defaults()
                                    .withOrdering(ordering)
                                    .withConcurrency(ordering == OrderingScope.SUBSCRIPTION ? 1 : 2),
                            ctx -> Disposition.accept())
                    .close());
        }
        for (DeduplicationScope scope : missingScopes) {
            assertUnimplemented(() -> transport
                    .sender(
                            SendTarget.subscription(id),
                            Scope.whole(),
                            SenderOptions.defaults().withDeduplication(scope))
                    .close());
        }
    }

    /**
     * Durable sends are the rule under at-least-once and transacted: a message that says it is not
     * durable is refused as invalid, and one that says nothing is stored durable.
     */
    @Test
    void explicitNonDurableUnderAtLeastOnceIsRefused() {
        assumeGuarantee(Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription("durable");
        OutboundMessage nonDurable =
                message(1).withHeaders(MessageHeaders.builder().durable(false).build());
        try (Sender sender = transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            assertThatThrownBy(() -> sender.send(nonDurable)).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
            });
            sender.send(message(2));
        }
        List<Message> stored = drain(transport, id, 1, SECONDS_15, QUIET);
        assertThat(seqs(stored)).containsExactly(2L);
        assertThat(stored.getFirst().headers().durable()).isNotEqualTo(Optional.of(false));
    }

    // Deduplication.

    /**
     * A message sent again with the same deduplication id, by the same sender or another, is stored
     * once, and a message with another id is not mistaken for it.
     */
    @Test
    void sameDeduplicationIdIsStoredOnce() {
        assumeWindowDeduplication();
        ResourceId id = newSubscription("dedup");
        MessageHeaders same = MessageHeaders.builder()
                .deduplicationId("tck-" + UUID.randomUUID())
                .build();
        MessageHeaders other = MessageHeaders.builder()
                .deduplicationId("tck-" + UUID.randomUUID())
                .build();
        try (Sender first = transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults());
                Sender second =
                        transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            sendTolerating(first, message(1, same));
            sendTolerating(first, message(1, same));
            sendTolerating(second, message(1, same));
            sendTolerating(second, message(2, other));
        }
        assertThat(seqs(drain(transport, id, 2, SECONDS_15, Duration.ofSeconds(2))))
                .containsExactlyInAnyOrder(1L, 2L);
    }

    /**
     * A sender reports the deduplication window of its destination when the transport deduplicates,
     * as a number of ids, a time or both, and reports none when it does not.
     */
    @Test
    void deduplicationWindowIsReported() {
        ResourceId id = newSubscription("window");
        try (Sender sender = transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            if (caps.deduplication().satisfies(DeduplicationScope.WINDOW)) {
                assertThat(sender.deduplication())
                        .hasValueSatisfying(window -> assertThat(window.ids().isPresent()
                                        || window.time().isPresent())
                                .as("a window states its size in ids or in time")
                                .isTrue());
            } else {
                assertThat(sender.deduplication()).isEmpty();
            }
        }
    }

    // Ordering.

    /**
     * Messages of one group reach the handler in the order sent and one at a time, even with several
     * lanes handling other groups at once.
     */
    @Test
    void groupOrderKeptWithConcurrency() {
        assumeOrdered(OrderingScope.GROUP);
        ResourceId id = newSubscription("group");
        int groups = 3;
        int perGroup = 200;
        List<OutboundMessage> messages = new ArrayList<>();
        for (int n = 0; n < perGroup; n++) {
            for (int g = 0; g < groups; g++) {
                messages.add(
                        message(n, MessageHeaders.builder().groupId("g" + g).build()));
            }
        }
        sendOrdered(id, OrderingScope.GROUP, messages);

        Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
        Map<String, List<Long>> order = new ConcurrentHashMap<>();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        own(transport.process(
                id,
                Scope.whole(),
                ProcessorOptions.defaults().withOrdering(OrderingScope.GROUP).withConcurrency(4),
                ctx -> {
                    String group = ctx.message().headers().groupId().orElse("none");
                    AtomicInteger inGroup = running.computeIfAbsent(group, k -> new AtomicInteger());
                    if (inGroup.incrementAndGet() > 1) {
                        overlaps.incrementAndGet();
                    }
                    try {
                        order.computeIfAbsent(group, k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(seq(ctx.message()));
                        Thread.sleep(1);
                    } finally {
                        inGroup.decrementAndGet();
                    }
                    handled.incrementAndGet();
                    return Disposition.accept();
                }));
        await("all messages handled", SECONDS_60, () -> handled.get() >= groups * perGroup);
        assertThat(overlaps).as("two messages of one group ran at once").hasValue(0);
        List<Long> expected = LongStream.range(0, perGroup).boxed().toList();
        for (int g = 0; g < groups; g++) {
            assertThat(order.get("g" + g)).as("group g" + g).containsExactlyElementsOf(expected);
        }
    }

    /** With subscription ordering the whole subscription is handled in the order sent. */
    @Test
    void subscriptionOrderKept() {
        assumeOrdered(OrderingScope.SUBSCRIPTION);
        ResourceId id = newSubscription("order");
        int count = 200;
        sendOrdered(
                id,
                OrderingScope.SUBSCRIPTION,
                LongStream.range(0, count).mapToObj(TckSupport::message).toList());

        List<Long> order = Collections.synchronizedList(new ArrayList<>());
        own(transport.process(
                id,
                Scope.whole(),
                ProcessorOptions.defaults().withConcurrency(1).withOrdering(OrderingScope.SUBSCRIPTION),
                ctx -> {
                    order.add(seq(ctx.message()));
                    return Disposition.accept();
                }));
        await("all messages handled", SECONDS_60, () -> order.size() >= count);
        assertThat(order)
                .containsExactlyElementsOf(LongStream.range(0, count).boxed().toList());
    }

    // Flow control.

    /**
     * A receiver never holds more unsettled messages than its credit, and with no prefetch the
     * messages it does not hold stay available to another receiver of the subscription.
     */
    @Test
    void receiverNeverHoldsMoreThanCredit() {
        ResourceId id = newSubscription("credit");
        sendDirect(id, 20);
        try (Receiver limited =
                transport.receiver(id, Scope.whole(), ReceiverOptions.consume().withFlow(new FlowControl(5, 0, 0)))) {
            List<Message> held = receiveN(limited, 5, SECONDS_15);
            assertThat(held).hasSize(5);
            assertThat(limited.receive(20, QUIET)).as("credit is used up").isEmpty();

            List<Message> rest;
            try (Receiver other = transport.receiver(id, Scope.whole(), ReceiverOptions.consume())) {
                rest = receiveN(other, 15, SECONDS_15);
                assertThat(rest).hasSize(15);
                rest.forEach(m -> other.settle(m, Disposition.accept()));
            }
            Set<Long> all = new HashSet<>(seqs(held));
            all.addAll(seqs(rest));
            assertThat(all).as("no message is held twice").hasSize(20);
            held.forEach(m -> limited.settle(m, Disposition.accept()));
        }
    }

    /**
     * A processor with {@code n} lanes runs exactly {@code n} handlers at once when more messages
     * wait: the concurrency is a ceiling that is also reached.
     */
    @Test
    void processorNeverRunsMoreThanConcurrency() throws Exception {
        ResourceId id = newSubscription("lanes");
        sendDirect(id, 100);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        try {
            own(transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(8), ctx -> {
                peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                try {
                    release.await();
                } finally {
                    running.decrementAndGet();
                }
                handled.incrementAndGet();
                return Disposition.accept();
            }));
            await("eight handlers running", SECONDS_15, () -> running.get() == 8);
            pause(Duration.ofMillis(500));
            assertThat(running).hasValue(8);
            assertThat(peak).hasValue(8);
        } finally {
            release.countDown();
        }
        await("all messages handled", SECONDS_60, () -> handled.get() == 100);
        assertThat(peak).hasValue(8);
    }

    /**
     * Releasing one message hands back only that message: the other lanes keep theirs, and the
     * released one is handled again.
     */
    @Test
    void releasingOneMessageHandsBackOnlyThatMessage() {
        ResourceId id = newSubscription("release-one");
        sendDirect(id, 3);
        Map<Long, AtomicInteger> calls = new ConcurrentHashMap<>();
        CountDownLatch hold = new CountDownLatch(1);
        try {
            own(transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(3), ctx -> {
                long n = seq(ctx.message());
                int call = calls.computeIfAbsent(n, k -> new AtomicInteger()).incrementAndGet();
                if (n == 1 && call == 1) {
                    return new Disposition.Release(Optional.empty(), false);
                }
                hold.await();
                return Disposition.accept();
            }));
            await(
                    "the released message handled again",
                    SECONDS_15,
                    () -> calls.containsKey(1L) && calls.get(1L).get() >= 2);
            pause(Duration.ofMillis(500));
            assertThat(calls.get(0L)).as("the held message is not handed back").hasValue(1);
            assertThat(calls.get(2L)).as("the held message is not handed back").hasValue(1);
        } finally {
            hold.countDown();
        }
        await("every message settled", SECONDS_15, () -> exactCount(id) == 0);
    }

    /**
     * Stopping a processor lets the handlers in flight finish, and hands back the messages it had
     * not started without counting a delivery against them.
     */
    @Test
    void stopLetsInFlightFinishAndHandsBackTheRest() throws Exception {
        ResourceId id = newSubscription("stop");
        sendDirect(id, 6);
        AtomicInteger running = new AtomicInteger();
        Set<Long> finished = ConcurrentHashMap.newKeySet();
        CountDownLatch release = new CountDownLatch(1);
        Processor processor = own(
                transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(2), ctx -> {
                    running.incrementAndGet();
                    release.await();
                    finished.add(seq(ctx.message()));
                    return Disposition.accept();
                }));
        await("two handlers running", SECONDS_15, () -> running.get() == 2);
        Thread stopper = Thread.ofVirtual().start(() -> processor.stop(Duration.ofSeconds(20)));
        pause(Duration.ofMillis(300));
        release.countDown();
        stopper.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(stopper.isAlive()).as("stop returned").isFalse();
        assertThat(finished).as("the handlers in flight finished").hasSize(2);

        List<Message> rest = drain(transport, id, 4, SECONDS_15, QUIET);
        assertThat(rest).hasSize(4);
        assertThat(seqs(rest)).doesNotContainAnyElementsOf(finished);
        assertThat(rest)
                .allSatisfy(m -> m.headers()
                        .deliveryCount()
                        .ifPresent(count ->
                                assertThat(count).as("handed back uncounted").isLessThanOrEqualTo(1)));
    }

    /** A sender that is not transacted is safe to use from several threads at once. */
    @Test
    void senderIsSafeFromSeveralThreads() throws Exception {
        assumeGuarantee(Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription("threads");
        int threads = 4;
        int each = 50;
        try (Sender sender = transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            List<Thread> workers = new ArrayList<>();
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            for (int t = 0; t < threads; t++) {
                long base = t * 1000L;
                workers.add(Thread.ofVirtual().start(() -> {
                    try {
                        for (int i = 0; i < each; i++) {
                            sender.send(message(base + i));
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                }));
            }
            for (Thread worker : workers) {
                worker.join(TimeUnit.SECONDS.toMillis(60));
            }
            assertThat(failures).isEmpty();
        }
        assertThat(drain(transport, id, threads * each, SECONDS_15, QUIET)).hasSize(threads * each);
    }

    /**
     * The default options open a sender, a receiver and a processor on every kind of subscription the
     * environment can make, each bounded and at least once, and a message goes through them. The
     * default retry policy becomes the system's own where it counts attempts, and a managed one where
     * it does not.
     */
    @Test
    void defaultOptionsOpenBoundedAtLeastOnce() {
        assumeGuarantee(Guarantee.AT_LEAST_ONCE);
        assertThat(SenderOptions.defaults().guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(ReceiverOptions.consume().guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(ProcessorOptions.defaults().receive().guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(env.subscriptionKinds()).isNotEmpty();
        for (KindId kind : env.subscriptionKinds()) {
            SubscriptionCapabilities declared = caps.subscription(kind);
            ResourceId id = newSubscription(kind, "defaults");
            AtomicInteger handled = new AtomicInteger();
            try (Sender sender =
                    transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
                Processor processor = own(transport.process(id, Scope.whole(), ProcessorOptions.defaults(), ctx -> {
                    handled.incrementAndGet();
                    return Disposition.accept();
                }));
                assertThat(sender.state()).isEqualTo(LinkState.OPEN);
                assertThat(processor.state()).isEqualTo(LinkState.OPEN);
                assertThat(processor.retry().mode())
                        .as("the default retry of %s", kind)
                        .isEqualTo(declared.retry().countsAttempts() ? RetryMode.SYSTEM : RetryMode.MANAGED);
                sender.send(message(1));
                await("the message handled on " + kind, SECONDS_15, () -> handled.get() == 1);
            }
            try (Receiver receiver = transport.receiver(id, Scope.whole(), ReceiverOptions.consume())) {
                assertThat(receiver.state()).isEqualTo(LinkState.OPEN);
            }
        }
    }

    /**
     * Asking for the system's own retry where the system counts no attempts is refused when the
     * processor is opened, as unimplemented and not sent, never accepted and then never given up.
     */
    @Test
    void systemRetryWhereNothingIsCountedIsUnimplemented() {
        List<KindId> uncounted = env.subscriptionKinds().stream()
                .filter(kind -> !caps.subscription(kind).retry().countsAttempts())
                .toList();
        assumeTrue(!uncounted.isEmpty(), "The system counts attempts on every subscription kind");
        for (KindId kind : uncounted) {
            ResourceId id = newSubscription(kind, "system-uncounted");
            assertUnimplemented(() -> transport
                    .process(
                            id,
                            Scope.whole(),
                            ProcessorOptions.defaults().withRetry(RetryPolicy.system()),
                            ctx -> Disposition.accept())
                    .close());
        }
    }

    // Retries and dead letters.

    /**
     * A rejection dead-letters the message at once: the handler is not called again, and the
     * message lands in the dead-letter destination with the reason.
     */
    @Test
    void rejectDeadLettersAtOnce() {
        ResourceId id = newSubscription("reject");
        ResourceId dead = newSubscription("reject-dead");
        sendDirect(id, 1);
        AtomicInteger calls = new AtomicInteger();
        own(transport.process(
                id,
                Scope.whole(),
                ProcessorOptions.defaults()
                        .withRetry(RetryPolicy.managed(3, Backoff.defaults())
                                .withDeadLetter(SendTarget.subscription(dead))),
                ctx -> {
                    calls.incrementAndGet();
                    return new Disposition.Reject(Optional.of("tck-reject"));
                }));
        List<Message> deadLettered = drain(transport, dead, 1, SECONDS_15, QUIET);
        assertThat(seqs(deadLettered)).containsExactly(0L);
        assertThat(deadLettered.getFirst().properties()).containsKey(DEAD_LETTER_REASON);
        assertThat(calls).as("a rejection is not retried").hasValue(1);
    }

    /**
     * A released message with a delay stays hidden until the delay has passed, and is then
     * delivered again.
     */
    @Test
    void releaseWithDelayIsHiddenUntilDue() {
        ResourceId id = newSubscription("delay");
        sendDirect(id, 1);
        Duration delay = Duration.ofSeconds(2);
        try (Receiver receiver = transport.receiver(id, Scope.whole(), ReceiverOptions.consume())) {
            List<Message> first = receiveN(receiver, 1, SECONDS_15);
            assertThat(first).hasSize(1);
            long released = System.nanoTime();
            receiver.settle(first.getFirst(), new Disposition.Release(Optional.of(delay), false));
            assertThat(receiver.receive(1, Duration.ofMillis(1000)))
                    .as("hidden until due")
                    .isEmpty();
            List<Message> again = receiveN(receiver, 1, SECONDS_15);
            assertThat(again).hasSize(1);
            assertThat(Duration.ofNanos(System.nanoTime() - released))
                    .as("delivered no earlier than the delay")
                    .isGreaterThanOrEqualTo(delay.minusMillis(100));
            receiver.settle(again.getFirst(), Disposition.accept());
        }
    }

    /**
     * Under a managed policy a failing handler is called {@code maxAttempts} times, at least the
     * backoff apart, and the message is then dead-lettered with the reason.
     */
    @Test
    void managedRetryBacksOffThenDeadLetters() {
        ResourceId id = newSubscription("managed");
        ResourceId dead = newSubscription("managed-dead");
        sendDirect(id, 1);
        Duration backoff = Duration.ofMillis(400);
        List<Long> times = Collections.synchronizedList(new ArrayList<>());
        own(transport.process(
                id,
                Scope.whole(),
                ProcessorOptions.defaults()
                        .withRetry(RetryPolicy.managed(3, new Backoff(backoff, 1.0, backoff, 0.0))
                                .withDeadLetter(SendTarget.subscription(dead))),
                ctx -> {
                    times.add(System.nanoTime());
                    throw new IllegalStateException("tck-fail");
                }));
        List<Message> deadLettered = drain(transport, dead, 1, SECONDS_15, QUIET);
        assertThat(seqs(deadLettered)).containsExactly(0L);
        assertThat(deadLettered.getFirst().properties())
                .containsKey(DEAD_LETTER_ATTEMPTS)
                .containsKey(DEAD_LETTER_REASON);
        assertThat(times).as("one call per attempt").hasSize(3);
        for (int i = 1; i < times.size(); i++) {
            assertThat(Duration.ofNanos(times.get(i) - times.get(i - 1)))
                    .as("the gap before attempt " + (i + 1))
                    .isGreaterThanOrEqualTo(backoff.minusMillis(50));
        }
    }

    /**
     * Ordered processing retries in place: a group's first message fails twice and the group's
     * later messages wait for it, so the order of handling is kept.
     */
    @Test
    void orderedManagedRetryKeepsOrder() {
        assumeOrdered(OrderingScope.GROUP);
        ResourceId id = newSubscription("ordered-retry");
        List<OutboundMessage> messages = LongStream.range(0, 5)
                .mapToObj(n -> message(n, MessageHeaders.builder().groupId("g").build()))
                .toList();
        sendOrdered(id, OrderingScope.GROUP, messages);
        List<Long> calls = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger done = new AtomicInteger();
        own(transport.process(
                id,
                Scope.whole(),
                ProcessorOptions.defaults()
                        .withOrdering(OrderingScope.GROUP)
                        .withConcurrency(2)
                        .withRetry(RetryPolicy.managed(
                                5, new Backoff(Duration.ofMillis(100), 1.0, Duration.ofMillis(100), 0.0))),
                ctx -> {
                    long n = seq(ctx.message());
                    calls.add(n);
                    if (n == 0 && ctx.attempt() < 3) {
                        throw new IllegalStateException("tck-fail");
                    }
                    done.incrementAndGet();
                    return Disposition.accept();
                }));
        await("every message handled", SECONDS_60, () -> done.get() == 5);
        assertThat(calls).containsExactly(0L, 0L, 0L, 1L, 2L, 3L, 4L);
    }

    /**
     * Under the system's policy a handler that throws releases its message as a counted delivery,
     * and the message is handled again, knowing it is a later attempt.
     */
    @Test
    void systemRetryReleasesCounted() {
        assumeTrue(subscription().retry().countsAttempts(), "The system counts no attempts");
        ResourceId id = newSubscription("system-retry");
        sendDirect(id, 1);
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
        List<Message> deliveries = Collections.synchronizedList(new ArrayList<>());
        own(transport.process(id, Scope.whole(), ProcessorOptions.defaults().withRetry(RetryPolicy.system()), ctx -> {
            attempts.add(ctx.attempt());
            deliveries.add(ctx.message());
            if (ctx.attempt() == 1) {
                throw new IllegalStateException("tck-fail");
            }
            return Disposition.accept();
        }));
        await("the message handled again", SECONDS_15, () -> attempts.size() >= 2);
        assertThat(attempts.getFirst()).isEqualTo(1);
        assertThat(attempts.get(1)).isGreaterThanOrEqualTo(2);
        deliveries
                .get(1)
                .headers()
                .deliveryCount()
                .ifPresent(count -> assertThat(count).isGreaterThanOrEqualTo(2));
    }

    // Security.

    /**
     * An account that may not use a destination is refused with a permission error naming the
     * destination, when sending and when receiving, and the refusal does not close the link: a
     * destination the account may use works afterwards.
     */
    @Test
    void unauthorizedDestinationIsPermissionDeniedWithResource() throws Exception {
        ConnectRequest restricted = env.restrictedRequest().orElse(null);
        assumeTrue(restricted != null, "The environment offers no restricted account");
        ResourceId denied = newSubscription("denied.x");
        ResourceId permitted = newSubscription("permitted");
        ClusterConnection limited = drivers.connect(restricted);
        own(limited);
        Transport limitedTransport = limited.transport().orElseThrow();

        try (Sender sender = openOrDenied(
                () -> limitedTransport.sender(SendTarget.subscription(denied), Scope.whole(), SenderOptions.defaults()),
                denied)) {
            if (sender != null) {
                assertDenied(() -> sender.send(message(1)), denied);
                Offer offer = sender.offer(message(2));
                if (offer instanceof Offer.Accepted accepted) {
                    assertThatThrownBy(() ->
                                    accepted.receipt().toCompletableFuture().get(10, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .satisfies(e -> assertIsDenied(e.getCause(), denied));
                }
                assertThat(sender.state()).as("a refusal leaves the link open").isEqualTo(LinkState.OPEN);
            }
        }
        assertDenied(
                () -> {
                    try (Receiver receiver =
                            limitedTransport.receiver(denied, Scope.whole(), ReceiverOptions.consume())) {
                        receiver.receive(1, Duration.ofSeconds(2));
                    }
                },
                denied);

        try (Sender sender =
                limitedTransport.sender(SendTarget.subscription(permitted), Scope.whole(), SenderOptions.defaults())) {
            sender.send(message(3));
        }
        assertThat(seqs(drain(transport, permitted, 1, SECONDS_15, QUIET))).containsExactly(3L);
    }

    /** A client that authenticates with a certificate connects, sends and receives. */
    @Test
    void mutualTlsConnects() {
        ConnectRequest request = env.mutualTlsRequest().orElse(null);
        assumeTrue(request != null, "The environment offers no mutual TLS endpoint");
        ResourceId id = newSubscription("mtls");
        ClusterConnection secure = drivers.connect(request);
        own(secure);
        Transport secureTransport = secure.transport().orElseThrow();
        try (Sender sender =
                secureTransport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            sender.send(message(7));
        }
        try (Receiver receiver = secureTransport.receiver(id, Scope.whole(), ReceiverOptions.consume())) {
            List<Message> got = receiveN(receiver, 1, SECONDS_15);
            assertThat(seqs(got)).containsExactly(7L);
            receiver.settle(got.getFirst(), Disposition.accept());
        }
    }

    // Observability.

    /**
     * The observer on a connection sees the send, the processing and the settlement, and the trace
     * context it injects into a sent message reaches the handler's message and the observer of the
     * processing, so a trace can link the two.
     */
    @Test
    void observerLinksProcessToSend() {
        ResourceId id = newSubscription("observer");
        RecordingObserver observer = new RecordingObserver();
        ConnectRequest base = env.request();
        ClusterConnection observed = drivers.connect(new ConnectRequest(
                base.system(),
                base.cluster(),
                base.credentials(),
                base.options().withObserver(observer)));
        own(observed);
        Transport observedTransport = observed.transport().orElseThrow();
        List<PropertyValue> carried = Collections.synchronizedList(new ArrayList<>());
        own(observedTransport.process(id, Scope.whole(), ProcessorOptions.defaults(), ctx -> {
            carried.add(ctx.message().properties().get(TRACEPARENT));
            return Disposition.accept();
        }));
        try (Sender sender =
                observedTransport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults())) {
            sender.send(message(1));
        }
        await("the message handled", SECONDS_15, () -> !carried.isEmpty());
        assertThat(carried).containsExactly(PropertyValue.of(observer.traceparent));
        await("the settlement observed", SECONDS_15, () -> observer.types.contains(OperationType.SETTLE));
        assertThat(observer.types).contains(OperationType.SEND, OperationType.PROCESS, OperationType.SETTLE);
        assertThat(observer.processed).containsExactly(observer.traceparent);
    }

    // Virtual threads.

    /**
     * Processing on virtual threads never pins a carrier thread: a recording of pinning events with
     * no threshold finds none while a processor handles messages.
     */
    @Test
    void processingDoesNotPinVirtualThreads() throws IOException {
        ResourceId id = newSubscription("pinning");
        int count = 200;
        sendDirect(id, count);
        Path file = Files.createTempFile("tck-pinning", ".jfr");
        try {
            AtomicInteger handled = new AtomicInteger();
            try (Recording recording = new Recording()) {
                recording
                        .enable("jdk.VirtualThreadPinned")
                        .withThreshold(Duration.ZERO)
                        .withStackTrace();
                recording.start();
                Processor processor = transport.process(
                        id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(4), ctx -> {
                            Thread.sleep(2);
                            handled.incrementAndGet();
                            return Disposition.accept();
                        });
                try {
                    await("all messages handled", SECONDS_60, () -> handled.get() >= count);
                } finally {
                    processor.stop(Duration.ofSeconds(10));
                    processor.close();
                }
                recording.stop();
                recording.dump(file);
            }
            List<RecordedEvent> pinned = RecordingFile.readAllEvents(file).stream()
                    .filter(e -> e.getEventType().getName().equals("jdk.VirtualThreadPinned"))
                    .toList();
            assertThat(pinned).as("pinned virtual threads: %s", pinned).isEmpty();
        } finally {
            Files.deleteIfExists(file);
        }
    }

    // Helpers.

    private static final String TRACEPARENT = "traceparent";

    /** The property a dead-lettered message carries, naming why it was dead-lettered. */
    private static final String DEAD_LETTER_REASON = "openbroker.dead_letter_reason";

    /** The property the engine puts on a message it dead-letters after the last attempt of a managed retry. */
    private static final String DEAD_LETTER_ATTEMPTS = "openbroker.dead_letter_attempts";

    private void assumeGuarantee(Guarantee guarantee) {
        assumeTrue(caps.guarantees().contains(guarantee), "The transport declares no " + guarantee);
    }

    /** Ordered at-least-once sending needs deduplication of at least {@code SESSION} scope. */
    private void assumeOrdered(OrderingScope ordering) {
        assumeGuarantee(Guarantee.AT_LEAST_ONCE);
        assumeTrue(
                caps.orderings().contains(ordering)
                        && subscription().orderings().contains(ordering),
                "The transport declares no " + ordering + " ordering");
        assumeTrue(
                caps.deduplication().satisfies(DeduplicationScope.SESSION),
                "The transport declares no deduplication of at least SESSION scope");
    }

    /** Caller-set deduplication ids are honoured only by a system that keeps a window of them. */
    private void assumeWindowDeduplication() {
        assumeTrue(
                caps.deduplication().satisfies(DeduplicationScope.WINDOW),
                "The transport declares no WINDOW deduplication");
    }

    /** What the transport declares on the environment's own subscription kind. */
    private SubscriptionCapabilities subscription() {
        return caps.subscription(env.subscriptionKind());
    }

    private <T extends AutoCloseable> T own(T closeable) {
        open.push(closeable);
        return closeable;
    }

    private ResourceId newSubscription(String tag) {
        return newSubscription(env.subscriptionKind(), tag);
    }

    private ResourceId newSubscription(KindId kind, String tag) {
        String name = "tck." + tag + "." + UUID.randomUUID().toString().substring(0, 8);
        ResourceId id = env.subscriptionId(kind, name);
        connection
                .operations()
                .execute(id, new Operation.Create(env.subscriptionAttributes(kind, name)), Scope.whole())
                .orThrow();
        created.add(id);
        return id;
    }

    /** Sends {@code count} messages numbered from 0 with the connection's own send, outside the transport. */
    private void sendDirect(ResourceId id, int count) {
        for (int i = 0; i < count; i++) {
            connection.messages().send(SendTarget.subscription(id), message(i));
        }
    }

    /** Sends in order through one deduplicating sender of the given ordering, and waits for every receipt. */
    private void sendOrdered(ResourceId id, OrderingScope ordering, List<OutboundMessage> messages) {
        try (Sender sender = transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults()
                        .withDeduplication(DeduplicationScope.SESSION)
                        .withOrdering(ordering))) {
            confirm(offerAll(sender, messages, SECONDS_60), SECONDS_60);
        }
    }

    private long exactCount(ResourceId id) {
        return switch (connection
                .messages()
                .count(id, Scope.whole(), Optional.empty())
                .orThrow()
                .count()) {
            case Count.Exact e -> e.value();
            case Count.Estimate e -> e.value();
            case Count.Unavailable u -> throw new AssertionError("Count unavailable: " + u.reason());
        };
    }

    private static void assertUnimplemented(ThrowingRunnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
            assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        });
    }

    private static void assertDenied(ThrowingRunnable action, ResourceId resource) {
        assertThatThrownBy(action::run).satisfies(e -> assertIsDenied(e, resource));
    }

    private static void assertIsDenied(Throwable failure, ResourceId resource) {
        BrokerException e = TckSupport.broker(failure);
        assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_AUTHORIZED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
        assertThat(e.info().metadata()).containsEntry(ErrorReasons.PLANE, "DATA");
        assertThat(e.info().metadata().get(ErrorReasons.RESOURCE)).contains(resource.name());
    }

    /** Opens a sender, or returns null when opening it is itself refused as the case expects. */
    private static Sender openOrDenied(Callable<Sender> open, ResourceId resource) throws Exception {
        try {
            return open.call();
        } catch (BrokerException e) {
            assertIsDenied(e, resource);
            return null;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Injects a fixed trace context into every send and records what the processing sees. */
    private static final class RecordingObserver implements MessagingObserver {

        final String traceparent = "00-" + UUID.randomUUID().toString().replace("-", "") + "-"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 16) + "-01";
        final List<OperationType> types = new CopyOnWriteArrayList<>();
        final List<String> processed = new CopyOnWriteArrayList<>();

        private static final Observation NOTHING = error -> {};

        @Override
        public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
            types.add(op.type());
            carrier.accept(TRACEPARENT, traceparent);
            return NOTHING;
        }

        @Override
        public Observation onReceive(OperationInfo op) {
            types.add(op.type());
            return NOTHING;
        }

        @Override
        public Observation onProcess(OperationInfo op, Message message) {
            types.add(op.type());
            PropertyValue carried = message.properties().get(TRACEPARENT);
            if (carried != null) {
                processed.add(carried.asText());
            }
            return NOTHING;
        }

        @Override
        public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
            types.add(op.type());
            return NOTHING;
        }
    }
}
