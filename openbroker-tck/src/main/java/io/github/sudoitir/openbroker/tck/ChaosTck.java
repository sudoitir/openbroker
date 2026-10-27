package io.github.sudoitir.openbroker.tck;

import static io.github.sudoitir.openbroker.tck.TckSupport.await;
import static io.github.sudoitir.openbroker.tck.TckSupport.confirm;
import static io.github.sudoitir.openbroker.tck.TckSupport.drain;
import static io.github.sudoitir.openbroker.tck.TckSupport.message;
import static io.github.sudoitir.openbroker.tck.TckSupport.offerAll;
import static io.github.sudoitir.openbroker.tck.TckSupport.pause;
import static io.github.sudoitir.openbroker.tck.TckSupport.receiveN;
import static io.github.sudoitir.openbroker.tck.TckSupport.results;
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
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendBuffer;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The chaos conformance kit: the contract for loss and duplicates while a broker is killed
 * mid-flight, the network to it is cut, or a high-availability pair fails over. Extend it in a
 * driver's integration test and return a {@link TckEnvironment} (for its driver and resource names)
 * and a {@link TckChaos} over a real cluster.
 *
 * <p>Each case starts from a healthy cluster, so {@link #chaos()} must return one that is fresh or
 * restored every time it is called. A case is skipped when the transport's capabilities declare what
 * it needs unavailable.
 */
public abstract class ChaosTck {

    private static final Duration SECONDS_15 = Duration.ofSeconds(15);
    private static final Duration SECONDS_60 = Duration.ofSeconds(60);
    private static final Duration SECONDS_120 = Duration.ofSeconds(120);
    private static final Duration QUIET = Duration.ofSeconds(2);

    private TckEnvironment env;
    private TckChaos chaos;
    private BrokerDrivers drivers;
    private final List<ClusterConnection> connections = new ArrayList<>();
    private final List<ResourceId> created = new ArrayList<>();
    private final Deque<AutoCloseable> open = new ArrayDeque<>();
    private final Set<NodeId> touched = new LinkedHashSet<>();

    /** The environment, for its driver and the names and attributes of subscriptions; called once per test. */
    protected abstract TckEnvironment environment();

    /** The cluster to disturb; called once per test, and healthy each time. */
    protected abstract TckChaos chaos();

    @BeforeEach
    void setUp() {
        env = environment();
        chaos = chaos();
        touched.add(chaos.node());
        drivers = BrokerDrivers.of(List.of(env.driver()));
    }

    @AfterEach
    void tearDown() {
        for (NodeId node : touched) {
            try {
                chaos.heal(node);
            } catch (RuntimeException ignored) {
                // Best effort: closing links must not wait on a cut.
            }
        }
        while (!open.isEmpty()) {
            try {
                open.pop().close();
            } catch (Exception ignored) {
                // Best effort: a case may have closed it already.
            }
        }
        for (ResourceId id : created) {
            for (ClusterConnection connection : connections) {
                try {
                    connection.operations().execute(id, new Operation.Delete(false, true), Scope.whole());
                    break;
                } catch (RuntimeException ignored) {
                    // Best effort: the node may be gone.
                }
            }
        }
        connections.forEach(ClusterConnection::close);
        drivers.close();
    }

    // Kill and restart.

    /**
     * A deduplicating at-least-once sender whose broker is killed and restarted mid-send delivers every
     * accepted message exactly once, in the order sent.
     */
    @Test
    void brokerKilledMidSendDeduplicatedIsExactlyOnce() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        assumeDeduplication(transport, DeduplicationScope.SESSION);
        ResourceId id = newSubscription(connection, "kill-idem");
        int count = 300;
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults()
                        .withDeduplication(DeduplicationScope.SESSION)
                        .withBuffer(roomy())));
        confirm(sendAcrossKill(sender, count, chaos.node(), true), SECONDS_120);
        List<Message> stored = drain(transport, id, count, SECONDS_60, QUIET);
        assertThat(seqs(stored)).containsExactlyElementsOf(range(count));
    }

    /**
     * An at-least-once sender whose broker is killed and restarted mid-send loses no confirmed
     * message, and every message stored twice was reported as sent again.
     */
    @Test
    void brokerKilledMidSendAtLeastOnceLosesNothing() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "kill-alo");
        int count = 300;
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults().withBuffer(roomy())));
        List<SendReceipt> receipts = confirm(sendAcrossKill(sender, count, chaos.node(), true), SECONDS_120);
        List<Long> stored = seqs(drain(transport, id, count, SECONDS_60, QUIET));

        assertThat(new LinkedHashSet<>(stored)).containsExactlyInAnyOrderElementsOf(range(count));
        Map<Long, Integer> times = new HashMap<>();
        stored.forEach(n -> times.merge(n, 1, Integer::sum));
        times.forEach((n, copies) -> {
            if (copies > 1) {
                assertThat(receipts.get(Math.toIntExact(n)).resent())
                        .as("message %d is stored %d times and says it was not sent again", n, copies)
                        .isTrue();
            }
        });
    }

    /**
     * An at-most-once sender whose broker is killed never sends again and so never duplicates: a
     * send either succeeded or failed as in doubt.
     */
    @Test
    void brokerKilledAtMostOnceNeverDuplicates() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_MOST_ONCE);
        ResourceId id = newSubscription(connection, "kill-amo");
        int count = 300;
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults().withGuarantee(Guarantee.AT_MOST_ONCE).withBuffer(roomy())));
        List<TckSupport.Result> results = results(sendAcrossKill(sender, count, chaos.node(), true), SECONDS_120);
        for (TckSupport.Result result : results) {
            result.receipt().ifPresent(receipt -> assertThat(receipt.resent()).isFalse());
            result.error().ifPresent(error -> {
                BrokerException e = TckSupport.broker(error);
                assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
                assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
            });
        }
        List<Long> stored = seqs(drain(transport, id, 0, SECONDS_60, QUIET));
        assertThat(stored).doesNotHaveDuplicates();
    }

    /**
     * A processor whose broker is killed and restarted mid-way still handles every message at least
     * once: nothing it had not settled is lost.
     */
    @Test
    void brokerKilledMidProcessLosesNothing() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "kill-proc");
        int count = 100;
        sendDirect(connection, id, count);
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        own(transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(4), ctx -> {
            Thread.sleep(10);
            seen.add(seq(ctx.message()));
            return Disposition.accept();
        }));
        await("a third handled", SECONDS_60, () -> seen.size() >= count / 3);
        chaos.kill(chaos.node());
        Thread restarter = restartAfter(chaos.node(), Duration.ofSeconds(1));
        await("every message handled", SECONDS_120, () -> seen.size() == count);
        join(restarter);
    }

    /**
     * A commit while the broker is killed fails as in doubt or as rolled back, never as a success it
     * cannot vouch for, and redoing the transaction with the same deduplication ids stores every
     * message exactly once.
     */
    @Test
    void commitDuringKillIsInDoubtOrAbortedAndRedoIsExactlyOnce() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.TRANSACTED);
        assumeDeduplication(transport, DeduplicationScope.WINDOW);
        ResourceId id = newSubscription(connection, "kill-tx");
        int count = 20;
        String run = UUID.randomUUID().toString();
        List<OutboundMessage> messages = LongStream.range(0, count)
                .mapToObj(n -> message(
                        n,
                        MessageHeaders.builder().deduplicationId(run + "-" + n).build()))
                .toList();
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults().withGuarantee(Guarantee.TRANSACTED)));
        messages.forEach(sender::send);

        chaos.kill(chaos.node());
        Thread restarter = restartAfter(chaos.node(), Duration.ofSeconds(1));
        assertThatThrownBy(sender::commit)
                .isInstanceOfSatisfying(
                        BrokerException.class,
                        e -> assertThat(e.dispatch() == Dispatch.IN_DOUBT
                                        || (e.status() == Status.ABORTED
                                                && e.reason().equals(ErrorReasons.TRANSACTION_ROLLED_BACK)))
                                .as("the commit failed as %s", e)
                                .isTrue());
        join(restarter);

        await("the redo committed", SECONDS_120, () -> {
            try {
                messages.forEach(m -> sendTolerating(sender, m));
                sender.commit();
                return true;
            } catch (BrokerException e) {
                try {
                    sender.rollback();
                } catch (BrokerException ignored) {
                    // The link is still recovering; the next try rolls back again.
                }
                return false;
            }
        });
        assertThat(seqs(drain(transport, id, count, SECONDS_60, QUIET))).containsExactlyElementsOf(range(count));
    }

    // Partitions.

    /**
     * A link whose network is cut goes to recovering and, once the network is back, to open again,
     * and the listener sees both, in that order, with the node that serves it.
     */
    @Test
    void partitionRecoversWithEvents() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "partition");
        List<LinkEvent> events = new CopyOnWriteArrayList<>();
        own(transport.watch(events::add));
        Sender sender = own(transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults()));

        chaos.cut(chaos.node());
        List<CompletionStage<SendReceipt>> receipts = offerAll(sender, List.of(message(0)), SECONDS_15);
        await(
                "the link recovering",
                SECONDS_60,
                () -> states(events, sender.id()).contains(LinkState.RECOVERING));
        chaos.heal(chaos.node());
        await("the link open again", SECONDS_60, () -> {
            List<LinkState> states = states(events, sender.id());
            return states.lastIndexOf(LinkState.OPEN) > states.indexOf(LinkState.RECOVERING);
        });
        confirm(receipts, SECONDS_60);
        assertThat(sender.state()).isEqualTo(LinkState.OPEN);
        assertThat(events.stream()
                        .filter(e -> e.link().equals(sender.id()) && e.state() == LinkState.OPEN)
                        .toList())
                .allSatisfy(e -> assertThat(e.servedBy()).contains(chaos.node()));
    }

    /**
     * A sender cut off from its broker accepts exactly as many messages as its buffer holds, then
     * reports full, and everything accepted is confirmed once the network is back.
     */
    @Test
    void partitionFillsSendBufferThenFull() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "full");
        int room = 10;
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults().withBuffer(new SendBuffer(room, 1024 * 1024, Duration.ofSeconds(60)))));

        chaos.cut(chaos.node());
        List<CompletionStage<SendReceipt>> accepted = new ArrayList<>();
        int full = 0;
        for (int i = 0; i < room + 15; i++) {
            if (sender.offer(message(i)) instanceof Offer.Accepted a) {
                accepted.add(a.receipt());
            } else {
                full++;
            }
        }
        assertThat(accepted).as("accepted while cut off").hasSize(room);
        assertThat(full).as("offers answered full").isEqualTo(15);

        chaos.heal(chaos.node());
        confirm(accepted, SECONDS_120);
        confirm(offerAll(sender, List.of(message(100)), SECONDS_60), SECONDS_60);
        assertThat(new LinkedHashSet<>(seqs(drain(transport, id, room + 1, SECONDS_60, QUIET))))
                .containsAll(LongStream.range(0, room).boxed().toList());
    }

    /**
     * A blocking send into a full buffer waits its configured time and then fails as exhausted and
     * not sent.
     */
    @Test
    void blockingSendOnAFullBufferFailsWithSendBufferFull() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "block");
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults().withBuffer(new SendBuffer(1, 1024 * 1024, Duration.ofSeconds(1)))));

        chaos.cut(chaos.node());
        assertThat(sender.offer(message(0))).isInstanceOf(Offer.Accepted.class);
        long started = System.nanoTime();
        assertThatThrownBy(() -> sender.send(message(1))).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
            assertThat(e.reason()).isEqualTo(ErrorReasons.SEND_BUFFER_FULL);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(900));
    }

    /**
     * A message received before the link recovered cannot be settled after it: the settle fails as
     * stale and not sent, and the message is delivered again.
     */
    @Test
    void staleSettleAfterRecoveryIsDeliveryStale() {
        ClusterConnection connection = connect(chaos.request());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "stale");
        int count = 3;
        sendDirect(connection, id, count);
        Receiver receiver = own(transport.receiver(id, Scope.whole(), ReceiverOptions.consume()));
        List<Message> before = receiveN(receiver, count, SECONDS_15);
        assertThat(before).hasSize(count);

        chaos.cut(chaos.node());
        await("the receiver recovering", SECONDS_60, () -> receiver.state() == LinkState.RECOVERING);
        chaos.heal(chaos.node());
        await("the receiver open again", SECONDS_60, () -> receiver.state() == LinkState.OPEN);

        assertThatThrownBy(() -> receiver.settle(before.getFirst(), Disposition.accept()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.DELIVERY_STALE);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                });
        List<Message> again = receiveN(receiver, count, SECONDS_120);
        assertThat(seqs(again)).containsExactlyInAnyOrderElementsOf(range(count));
        again.forEach(m -> receiver.settle(m, Disposition.accept()));
    }

    // Failover.

    /**
     * When the serving node of a high-availability pair fails and its successor takes over, a processor at
     * least once still handles every message: none is lost. The processor's links move to the
     * successor.
     */
    @Test
    void failoverAtLeastOnceLosesNothing() {
        ClusterConnection connection = connect(chaos.haRequest());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        assumeTrue(transport.capabilities().failover(), "The transport declares no failover");
        ResourceId id = newSubscription(connection, "failover");
        int count = 100;
        sendDirect(connection, id, count);
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        NodeId serving = serving(id);
        own(transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(4), ctx -> {
            Thread.sleep(20);
            seen.add(seq(ctx.message()));
            return Disposition.accept();
        }));
        await("some handled", SECONDS_60, () -> seen.size() >= 10);
        chaos.kill(serving);
        await("every message handled", SECONDS_120, () -> seen.size() == count);
    }

    /**
     * A deduplicating at-least-once sender whose serving node fails over to its successor mid-send stores
     * every accepted message exactly once, in the order sent.
     */
    @Test
    void failoverDeduplicatedSendIsExactlyOnce() {
        ClusterConnection connection = connect(chaos.haRequest());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        assumeTrue(transport.capabilities().failover(), "The transport declares no failover");
        assumeDeduplication(transport, DeduplicationScope.SESSION);
        ResourceId id = newSubscription(connection, "failover-idem");
        int count = 300;
        Sender sender = own(transport.sender(
                SendTarget.subscription(id),
                Scope.whole(),
                SenderOptions.defaults()
                        .withDeduplication(DeduplicationScope.SESSION)
                        .withBuffer(roomy())));
        confirm(sendAcrossKill(sender, count, serving(id), false), SECONDS_120);
        assertThat(seqs(drain(transport, id, count, SECONDS_60, QUIET))).containsExactlyElementsOf(range(count));
    }

    /** After a failover the listener sees the link recovering and then open, served by the successor. */
    @Test
    void failoverEventNamesServingBackup() {
        ClusterConnection connection = connect(chaos.haRequest());
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        assumeTrue(transport.capabilities().failover(), "The transport declares no failover");
        ResourceId id = newSubscription(connection, "failover-event");
        List<LinkEvent> events = new CopyOnWriteArrayList<>();
        own(transport.watch(events::add));
        Sender sender = own(transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults()));
        await("the link open", SECONDS_15, () -> sender.state() == LinkState.OPEN);

        NodeId serving = serving(id);
        NodeId successor = successor(id);
        chaos.kill(serving);
        await(
                "the link served by the successor",
                SECONDS_120,
                () -> events.stream()
                        .anyMatch(e -> e.link().equals(sender.id())
                                && e.state() == LinkState.OPEN
                                && e.servedBy().filter(successor::equals).isPresent()));
        List<LinkEvent> mine =
                events.stream().filter(e -> e.link().equals(sender.id())).toList();
        assertThat(mine.stream().map(LinkEvent::state).toList()).contains(LinkState.RECOVERING);
        assertThat(mine).allSatisfy(e -> assertThat(e.node()).isEqualTo(serving));
        confirm(offerAll(sender, List.of(message(1)), SECONDS_60), SECONDS_60);
    }

    // Credentials.

    /**
     * Receivers keep processing while credentials are replaced under load, and a link lost afterwards
     * asks the credential source again when it recovers, then goes on processing.
     */
    @Test
    void credentialRotationKeepsReceiversAndRecoversWithNewCredentials() {
        ConnectRequest base = chaos.request();
        Credentials initial = base.credentials().forPlane(Plane.DATA);
        AtomicReference<Credentials> current = new AtomicReference<>(initial);
        AtomicInteger asked = new AtomicInteger();
        CredentialSource source = plane -> {
            if (plane == Plane.DATA) {
                asked.incrementAndGet();
            }
            return plane == Plane.DATA ? current.get() : base.credentials().forPlane(plane);
        };
        ClusterConnection connection =
                connect(new ConnectRequest(base.system(), base.cluster(), source, base.options()));
        Transport transport = transport(connection);
        assumeGuarantee(transport, Guarantee.AT_LEAST_ONCE);
        ResourceId id = newSubscription(connection, "rotation");
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        Sender sender = own(transport.sender(SendTarget.subscription(id), Scope.whole(), SenderOptions.defaults()));
        Processor processor = own(
                transport.process(id, Scope.whole(), ProcessorOptions.defaults().withConcurrency(2), ctx -> {
                    seen.add(seq(ctx.message()));
                    return Disposition.accept();
                }));
        confirm(offerAll(sender, range(0, 20), SECONDS_60), SECONDS_60);
        await("the first batch handled", SECONDS_60, () -> seen.size() == 20);

        int askedBefore = asked.get();
        current.set(initial);
        assertThat(processor.state()).as("a rotation closes no link").isEqualTo(LinkState.OPEN);
        confirm(offerAll(sender, range(20, 40), SECONDS_60), SECONDS_60);
        await("the second batch handled", SECONDS_60, () -> seen.size() == 40);

        chaos.cut(chaos.node());
        await("the processor recovering", SECONDS_60, () -> processor.state() == LinkState.RECOVERING);
        chaos.heal(chaos.node());
        await("the processor open again", SECONDS_60, () -> processor.state() == LinkState.OPEN);
        assertThat(asked.get()).as("the source is asked again on recovery").isGreaterThan(askedBefore);
        confirm(offerAll(sender, range(40, 60), SECONDS_60), SECONDS_60);
        await("the third batch handled", SECONDS_60, () -> seen.size() == 60);
    }

    // Helpers.

    private ClusterConnection connect(ConnectRequest request) {
        ClusterConnection connection = drivers.connect(request);
        connections.add(connection);
        return connection;
    }

    private static Transport transport(ClusterConnection connection) {
        Transport transport = connection.transport().orElse(null);
        assumeTrue(transport != null, "The driver offers no transport");
        return transport;
    }

    /** The node serving {@code subscription}, resolved now and healed when the case ends. */
    private NodeId serving(ResourceId subscription) {
        NodeId node = chaos.serving(subscription);
        touched.add(node);
        return node;
    }

    /** The node that takes over for {@code subscription}, resolved now and healed when the case ends. */
    private NodeId successor(ResourceId subscription) {
        NodeId node = chaos.successor(subscription);
        touched.add(node);
        return node;
    }

    private static void assumeGuarantee(Transport transport, Guarantee guarantee) {
        assumeTrue(transport.capabilities().guarantees().contains(guarantee), "The transport declares no " + guarantee);
    }

    private static void assumeDeduplication(Transport transport, DeduplicationScope minimum) {
        assumeTrue(
                transport.capabilities().deduplication().satisfies(minimum),
                "The transport declares no deduplication of at least " + minimum + " scope");
    }

    private <T extends AutoCloseable> T own(T closeable) {
        open.push(closeable);
        return closeable;
    }

    private ResourceId newSubscription(ClusterConnection connection, String tag) {
        ResourceId id = env.subscriptionId(
                "tck." + tag + "." + UUID.randomUUID().toString().substring(0, 8));
        connection
                .operations()
                .execute(id, new Operation.Create(env.subscriptionAttributes(id.name())), Scope.whole())
                .orThrow();
        created.add(id);
        return id;
    }

    private static void sendDirect(ClusterConnection connection, ResourceId id, int count) {
        for (int i = 0; i < count; i++) {
            connection.messages().send(SendTarget.subscription(id), message(i));
        }
    }

    /** Room for every message of a case, so that a kill never finds the buffer the limit. */
    private static SendBuffer roomy() {
        return new SendBuffer(50, 16L * 1024 * 1024, Duration.ofSeconds(120));
    }

    private static List<Long> range(int count) {
        return LongStream.range(0, count).boxed().toList();
    }

    private static List<OutboundMessage> range(int from, int to) {
        return IntStream.range(from, to).mapToObj(TckSupport::message).toList();
    }

    /**
     * Offers {@code count} messages, kills the node after the first third is accepted, and offers
     * the rest while it is down. When {@code restart} is set the node is started again a second
     * after the kill. Returns the receipts in order.
     */
    private List<CompletionStage<SendReceipt>> sendAcrossKill(Sender sender, int count, NodeId node, boolean restart) {
        List<OutboundMessage> all =
                LongStream.range(0, count).mapToObj(TckSupport::message).toList();
        List<CompletionStage<SendReceipt>> receipts =
                new ArrayList<>(offerAll(sender, all.subList(0, count / 3), SECONDS_60));
        chaos.kill(node);
        Thread restarter = restart ? restartAfter(node, Duration.ofSeconds(1)) : null;
        receipts.addAll(offerAll(sender, all.subList(count / 3, count), SECONDS_120));
        if (restarter != null) {
            join(restarter);
        }
        return receipts;
    }

    private Thread restartAfter(NodeId node, Duration delay) {
        return Thread.ofVirtual().start(() -> {
            pause(delay);
            chaos.start(node);
        });
    }

    private static void join(Thread thread) {
        try {
            thread.join(TimeUnit.SECONDS.toMillis(120));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
        assertThat(thread.isAlive()).as("the node restarted").isFalse();
    }

    private static List<LinkState> states(List<LinkEvent> events, String link) {
        return events.stream()
                .filter(e -> e.link().equals(link))
                .map(LinkEvent::state)
                .toList();
    }
}
