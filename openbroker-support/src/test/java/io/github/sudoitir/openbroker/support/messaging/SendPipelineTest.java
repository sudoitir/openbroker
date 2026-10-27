package io.github.sudoitir.openbroker.support.messaging;

import static io.github.sudoitir.openbroker.support.messaging.Fakes.NODE;
import static io.github.sudoitir.openbroker.support.messaging.Fakes.outbound;
import static io.github.sudoitir.openbroker.support.messaging.Fakes.receipt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.SendBuffer;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeClock;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeSleeper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SendPipelineTest {

    private static final long WAIT = 5;

    /** A writer that hands every write to the test and confirms when told to. */
    private static final class Wire implements SendPipeline.Writer {
        final LinkedBlockingQueue<Written> written = new LinkedBlockingQueue<>();
        volatile boolean down;

        @Override
        public void write(OutboundMessage message, SendPipeline.Confirmation confirmation) {
            if (down) {
                throw new BrokerException(
                        Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, NODE, "down");
            }
            written.add(new Written(message, confirmation));
        }

        Written next() throws InterruptedException {
            Written w = written.poll(WAIT, TimeUnit.SECONDS);
            assertThat(w).as("a write within %d s", WAIT).isNotNull();
            return w;
        }
    }

    private record Written(OutboundMessage message, SendPipeline.Confirmation confirmation) {}

    private final FakeClock clock = new FakeClock();
    private final List<SendPipeline> opened = new ArrayList<>();
    private final List<BrokerException> lost = new CopyOnWriteArrayList<>();

    @AfterEach
    void closeAll() {
        opened.forEach(p -> p.fail(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, NODE, "test over")));
    }

    private SendPipeline pipeline(SenderOptions options, Wire wire) {
        return pipeline(options, wire, Fakes.context(clock, new FakeSleeper(clock)));
    }

    private SendPipeline pipeline(SenderOptions options, Wire wire, EngineContext context) {
        SendPipeline p =
                SendPipeline.open("test", NODE, options, Fakes.info(OperationType.SEND), context, wire, lost::add);
        opened.add(p);
        return p;
    }

    private static SenderOptions options(int maxInFlight, long maxBytes, Duration maxBlock) {
        return SenderOptions.defaults().withBuffer(new SendBuffer(maxInFlight, maxBytes, maxBlock));
    }

    private static SendReceipt await(CompletionStage<SendReceipt> stage) throws Exception {
        return stage.toCompletableFuture().get(WAIT, TimeUnit.SECONDS);
    }

    private static CompletionStage<SendReceipt> accepted(Offer offer) {
        return ((Offer.Accepted) offer).receipt();
    }

    private static BrokerException failureOf(CompletionStage<SendReceipt> stage) {
        assertThatThrownBy(() -> stage.toCompletableFuture().get(WAIT, TimeUnit.SECONDS))
                .hasCauseInstanceOf(BrokerException.class);
        try {
            stage.toCompletableFuture().get();
            throw new AssertionError("expected a failure");
        } catch (ExecutionException e) {
            return (BrokerException) e.getCause();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void offerIsBoundedByCount() throws Exception {
        SendPipeline p = pipeline(options(2, 1 << 20, Duration.ofSeconds(1)), new Wire());
        assertThat(p.offer(outbound("a"))).isInstanceOf(Offer.Accepted.class);
        assertThat(p.offer(outbound("b"))).isInstanceOf(Offer.Accepted.class);
        assertThat(p.offer(outbound("c"))).isEqualTo(new Offer.Full(2, 2));
    }

    @Test
    void offerIsBoundedByBytes() {
        SendPipeline p = pipeline(options(100, 10, Duration.ofSeconds(1)), new Wire());
        assertThat(p.offer(outbound(6))).isInstanceOf(Offer.Accepted.class);
        assertThat(p.offer(outbound(6))).isEqualTo(new Offer.Full(1, 6));
        assertThat(p.offer(outbound(4))).isInstanceOf(Offer.Accepted.class);
    }

    @Test
    void aMessageBiggerThanTheWindowIsAcceptedWhenTheWindowIsEmpty() {
        SendPipeline p = pipeline(options(100, 10, Duration.ofSeconds(1)), new Wire());
        assertThat(p.offer(outbound(50))).isInstanceOf(Offer.Accepted.class);
    }

    @Test
    void sendFailsNotSentWhenNoRoomWithinMaxBlock() {
        SendPipeline p = pipeline(options(1, 1 << 20, Duration.ofMillis(30)), new Wire());
        p.offer(outbound("held"));
        assertThatThrownBy(() -> p.send(outbound("late"))).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
            assertThat(e.reason()).isEqualTo(ErrorReasons.SEND_BUFFER_FULL);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        });
        assertThat(p.inFlight()).isEqualTo(1);
    }

    @Test
    void sendWaitsForRoomThenForTheConfirmation() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(1, 1 << 20, Duration.ofSeconds(WAIT)), wire);
        CompletionStage<SendReceipt> first = accepted(p.offer(outbound("one")));
        CompletableFuture<SendReceipt> second = CompletableFuture.supplyAsync(() -> p.send(outbound("two")));
        Written one = wire.next();
        assertThat(second).isNotDone();
        one.confirmation().confirmed(receipt("1"));
        assertThat(await(first).messageId()).contains("1");
        Written two = wire.next();
        two.confirmation().confirmed(receipt("2"));
        assertThat(second.get(WAIT, TimeUnit.SECONDS).messageId()).contains("2");
    }

    @Test
    void flushWaitsForEveryConfirmationElseDeadlineExceeded() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofSeconds(1)), wire);
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("x")));
        Written w = wire.next();
        assertThatThrownBy(() -> p.flush(Duration.ofMillis(20))).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
            assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
        });
        CompletableFuture<Void> flushed = CompletableFuture.runAsync(() -> p.flush(Duration.ofSeconds(WAIT)));
        w.confirmation().confirmed(receipt("x"));
        flushed.get(WAIT, TimeUnit.SECONDS);
        assertThat(await(r).messageId()).contains("x");
    }

    @Test
    void unconfirmedAtLeastOnceMessagesAreWrittenAgainInOrderAndMarkedResent() throws Exception {
        Wire first = new Wire();
        SenderOptions o = options(10, 1 << 20, Duration.ofSeconds(1))
                .withDeduplication(DeduplicationScope.SESSION)
                .withOrdering(OrderingScope.GROUP);
        SendPipeline p = pipeline(o, first);
        CompletionStage<SendReceipt> r1 = accepted(p.offer(outbound("m1")));
        CompletionStage<SendReceipt> r2 = accepted(p.offer(outbound("m2")));
        CompletionStage<SendReceipt> r3 = accepted(p.offer(outbound("m3")));
        Written w1 = first.next();
        Written w2 = first.next();
        Written w3 = first.next();
        w1.confirmation().confirmed(receipt("1"));
        assertThat(await(r1).resent()).isFalse();

        p.suspend(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, NODE, "lost"));
        CompletionStage<SendReceipt> r4 = accepted(p.offer(outbound("m4")));
        assertThat(first.written).isEmpty();

        Wire second = new Wire();
        p.resume(second);
        Written again2 = second.next();
        Written again3 = second.next();
        Written new4 = second.next();
        assertThat(again2.message().headers().deduplicationId())
                .isPresent()
                .isEqualTo(w2.message().headers().deduplicationId());
        assertThat(again3.message().headers().deduplicationId())
                .isEqualTo(w3.message().headers().deduplicationId());
        assertThat(new4.message().headers().deduplicationId())
                .isNotEqualTo(again3.message().headers().deduplicationId());

        w2.confirmation().confirmed(receipt("stale"));
        assertThat(r2).isNotDone();
        again2.confirmation().confirmed(receipt("2"));
        again3.confirmation().confirmed(receipt("3"));
        new4.confirmation().confirmed(receipt("4"));
        assertThat(await(r2).resent()).isTrue();
        assertThat(await(r3).resent()).isTrue();
        assertThat(await(r4).resent()).isFalse();
    }

    @Test
    void atMostOnceIsNeverWrittenAgain() throws Exception {
        Wire first = new Wire();
        SendPipeline p =
                pipeline(options(10, 1 << 20, Duration.ofSeconds(1)).withGuarantee(Guarantee.AT_MOST_ONCE), first);
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("once")));
        first.next();
        p.suspend(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, NODE, "lost"));
        BrokerException failure = failureOf(r);
        assertThat(failure.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(failure.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
        Wire second = new Wire();
        p.resume(second);
        assertThat(second.written).isEmpty();
        assertThat(p.inFlight()).isZero();
    }

    @Test
    void transactedSendsRollBackWithTheLink() throws Exception {
        Wire first = new Wire();
        SendPipeline p =
                pipeline(options(10, 1 << 20, Duration.ofSeconds(1)).withGuarantee(Guarantee.TRANSACTED), first);
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("tx")));
        first.next();
        p.suspend(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, NODE, "lost"));
        BrokerException failure = failureOf(r);
        assertThat(failure.status()).isEqualTo(Status.ABORTED);
        assertThat(failure.reason()).isEqualTo(ErrorReasons.TRANSACTION_ROLLED_BACK);
        assertThat(failure.dispatch()).isEqualTo(Dispatch.REJECTED);
    }

    @Test
    void orderingNeedsSessionDeduplicationForAtLeastOnce() {
        SenderOptions o = SenderOptions.defaults().withOrdering(OrderingScope.PARTITION);
        assertThatThrownBy(() -> pipeline(o, new Wire())).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        });
        pipeline(o.withDeduplication(DeduplicationScope.SESSION), new Wire());
        pipeline(o.withDeduplication(DeduplicationScope.WINDOW), new Wire());
        pipeline(o.withGuarantee(Guarantee.AT_MOST_ONCE), new Wire());
    }

    @Test
    void aWriteThatFindsTheLinkDownWaitsForTheRecoveryAndReportsTheLoss() throws Exception {
        Wire down = new Wire();
        down.down = true;
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofSeconds(1)), down);
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("later")));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT);
        while (lost.isEmpty() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(lost).hasSize(1);
        assertThat(r).isNotDone();

        Wire up = new Wire();
        p.resume(up);
        Written w = up.next();
        w.confirmation().confirmed(receipt("ok"));
        SendReceipt done = await(r);
        assertThat(done.resent()).isFalse();
    }

    @Test
    void aFailedLinkFailsEveryPendingReceiptAndRefusesNewSends() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofSeconds(1)), wire);
        CompletionStage<SendReceipt> written = accepted(p.offer(outbound("w")));
        wire.next();
        p.suspend(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, NODE, "lost"));
        CompletionStage<SendReceipt> queued = accepted(p.offer(outbound("q")));
        p.fail(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, NODE, "gave up"));
        assertThat(failureOf(written).dispatch()).isEqualTo(Dispatch.IN_DOUBT);
        BrokerException notWritten = failureOf(queued);
        assertThat(notWritten.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(notWritten.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        assertThatThrownBy(() -> p.send(outbound("after")))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNAVAILABLE));
        assertThatThrownBy(() -> p.offer(outbound("after"))).isInstanceOf(BrokerException.class);
    }

    @Test
    void aRefusedMessageFailsOnlyItsOwnReceipt() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofSeconds(1)), wire);
        CompletionStage<SendReceipt> bad = accepted(p.offer(outbound("bad")));
        CompletionStage<SendReceipt> good = accepted(p.offer(outbound("good")));
        wire.next()
                .confirmation()
                .failed(new BrokerException(
                        Status.RESOURCE_EXHAUSTED,
                        ErrorInfo.of(ErrorReasons.DESTINATION_FULL),
                        Dispatch.REJECTED,
                        NODE,
                        "full"));
        wire.next().confirmation().confirmed(receipt("g"));
        assertThat(failureOf(bad).reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
        assertThat(await(good).messageId()).contains("g");
    }

    @Test
    void receiptsCompleteOnAVirtualThreadNotOnTheConfirmingOne() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofSeconds(1)), wire);
        AtomicBoolean virtual = new AtomicBoolean();
        AtomicReference<Thread> completer = new AtomicReference<>();
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("x"))).whenComplete((receipt, error) -> {
            completer.set(Thread.currentThread());
            virtual.set(Thread.currentThread().isVirtual());
        });
        Thread confirming = Thread.ofPlatform().unstarted(() -> {
            try {
                wire.next().confirmation().confirmed(receipt("x"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        confirming.start();
        await(r);
        confirming.join();
        assertThat(virtual).isTrue();
        assertThat(completer.get()).isNotSameAs(confirming);
    }

    @Test
    void deduplicatingSendersGiveEveryMessageAnIdAndKeepTheCallersOwn() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(
                options(10, 1 << 20, Duration.ofSeconds(1)).withDeduplication(DeduplicationScope.SESSION), wire);
        p.offer(outbound("a"));
        p.offer(outbound("b")
                .withHeaders(MessageHeaders.builder().deduplicationId("mine").build()));
        assertThat(wire.next().message().headers().deduplicationId().orElseThrow())
                .matches("[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");
        assertThat(wire.next().message().headers().deduplicationId()).contains("mine");
    }

    @Test
    void closeFlushesThenCancelsTheRest() throws Exception {
        Wire wire = new Wire();
        SendPipeline p = pipeline(options(10, 1 << 20, Duration.ofMillis(20)), wire);
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("x")));
        wire.next();
        p.close();
        BrokerException failure = failureOf(r);
        assertThat(failure.status()).isEqualTo(Status.CANCELLED);
        assertThatThrownBy(() -> p.offer(outbound("y"))).isInstanceOf(BrokerException.class);
    }

    @Test
    void theObserverSeesEverySendInjectsItsCarrierAndEndsWithTheOutcome() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        AtomicInteger ended = new AtomicInteger();
        MessagingObserver observer = new MessagingObserver() {
            @Override
            public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
                seen.add(op.operation());
                carrier.accept("traceparent", "00-abc");
                return error -> ended.incrementAndGet();
            }

            @Override
            public Observation onReceive(OperationInfo op) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Observation onProcess(OperationInfo op, Message message) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
                throw new UnsupportedOperationException();
            }
        };
        Wire wire = new Wire();
        SendPipeline p = pipeline(
                options(10, 1 << 20, Duration.ofSeconds(1)),
                wire,
                Fakes.context(clock, new FakeSleeper(clock), observer));
        CompletionStage<SendReceipt> r = accepted(p.offer(outbound("x")));
        Written w = wire.next();
        assertThat(w.message().properties()).containsEntry("traceparent", PropertyValue.of("00-abc"));
        w.confirmation().confirmed(receipt("x"));
        await(r);
        assertThat(seen).containsExactly("send");
        assertThat(ended.get()).isEqualTo(1);
    }

    @Test
    void uuidV7CarriesTheClockAndTheVersion() {
        FakeClock fixed = new FakeClock();
        String id = SendPipeline.uuidV7(fixed, new Random(1));
        assertThat(id).startsWith(String.format("%08x-%04x", fixed.millis() >>> 16, fixed.millis() & 0xFFFF));
        assertThat(id.charAt(14)).isEqualTo('7');
    }
}
