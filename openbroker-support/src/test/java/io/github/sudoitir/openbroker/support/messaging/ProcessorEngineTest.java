package io.github.sudoitir.openbroker.support.messaging;

import static io.github.sudoitir.openbroker.support.messaging.Fakes.NODE;
import static io.github.sudoitir.openbroker.support.messaging.Fakes.delivered;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeClock;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeSleeper;
import io.github.sudoitir.openbroker.support.messaging.Fakes.RecordingSender;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ProcessorEngineTest {

    private static final long WAIT = 5;
    private static final Backoff PLAIN = new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(10), 0.0);

    private record Settled(Message message, Disposition disposition) {}

    /** Lanes that pull from one shared queue and record every settlement. */
    private static final class FakeLanes implements ProcessorEngine.Lanes {
        final LinkedBlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Settled> settled = new LinkedBlockingQueue<>();
        final RecordingSender deadLetter = new RecordingSender();
        final AtomicInteger closedLanes = new AtomicInteger();
        final boolean pins;

        FakeLanes(boolean pins) {
            this.pins = pins;
        }

        @Override
        public ProcessorEngine.Lane open(int index) {
            return new ProcessorEngine.Lane() {
                @Override
                public Optional<Message> next(Duration timeout) throws InterruptedException {
                    return Optional.ofNullable(inbox.poll(timeout.toNanos(), TimeUnit.NANOSECONDS));
                }

                @Override
                public void settle(Message message, Disposition disposition) {
                    settled.add(new Settled(message, disposition));
                }

                @Override
                public Sender sender(SendTarget target) {
                    return deadLetter;
                }

                @Override
                public void close() {
                    closedLanes.incrementAndGet();
                }
            };
        }

        @Override
        public boolean pinsGroups() {
            return pins;
        }

        @Override
        public LinkState state() {
            return LinkState.OPEN;
        }

        @Override
        public Sender deadLetter(SendTarget target) {
            return deadLetter;
        }

        Settled settled() throws InterruptedException {
            Settled s = settled.poll(WAIT, TimeUnit.SECONDS);
            assertThat(s).as("a settlement within %d s", WAIT).isNotNull();
            return s;
        }
    }

    private final FakeClock clock = new FakeClock();
    private final FakeSleeper sleeper = new FakeSleeper(clock);
    private final List<ProcessorEngine> started = new ArrayList<>();

    @AfterEach
    void stopAll() {
        started.forEach(engine -> engine.stop(Duration.ofMillis(200)));
    }

    private ProcessorEngine start(ProcessorOptions options, FakeLanes lanes, MessageHandler handler) {
        return start(options, lanes, handler, Fakes.context(clock, sleeper));
    }

    private ProcessorEngine start(
            ProcessorOptions options, FakeLanes lanes, MessageHandler handler, EngineContext context) {
        ProcessorEngine engine =
                ProcessorEngine.start("p", options, Fakes.info(OperationType.PROCESS), context, lanes, handler);
        started.add(engine);
        return engine;
    }

    private static ProcessorOptions options(int concurrency, OrderingScope ordering, RetryPolicy retry) {
        return ProcessorOptions.defaults()
                .withConcurrency(concurrency)
                .withOrdering(ordering)
                .withRetry(retry);
    }

    @Test
    void anAcceptedMessageIsSettledAccepted() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        start(options(2, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> Disposition.accept());
        Message m = delivered("m1", null, 0);
        lanes.inbox.add(m);
        Settled s = lanes.settled();
        assertThat(s.message()).isSameAs(m);
        assertThat(s.disposition()).isEqualTo(Disposition.accept());
    }

    @Test
    void activeHandlersNeverExceedConcurrency() throws Exception {
        int concurrency = 3;
        FakeLanes lanes = new FakeLanes(false);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        LinkedBlockingQueue<CountDownLatch> entered = new LinkedBlockingQueue<>();
        CountDownLatch release = new CountDownLatch(1);
        start(options(concurrency, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> {
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            entered.add(release);
            release.await();
            active.decrementAndGet();
            return Disposition.accept();
        });
        for (int i = 0; i < 12; i++) {
            lanes.inbox.add(delivered("m" + i, null, 0));
        }
        for (int i = 0; i < concurrency; i++) {
            assertThat(entered.poll(WAIT, TimeUnit.SECONDS)).isNotNull();
        }
        release.countDown();
        for (int i = 0; i < 12; i++) {
            lanes.settled();
        }
        assertThat(peak.get()).isEqualTo(concurrency);
    }

    @Test
    void messagesOfOneGroupNeverRunTogetherWhenTheSystemDoesNotPinGroups() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        Map<String, AtomicInteger> active = new ConcurrentHashMap<>();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger groupsAtOnce = new AtomicInteger();
        AtomicInteger peakGroups = new AtomicInteger();
        CountDownLatch bothGroups = new CountDownLatch(2);
        start(options(4, OrderingScope.GROUP, RetryPolicy.system()), lanes, ctx -> {
            String group = ctx.message().headers().groupId().orElseThrow();
            if (active.computeIfAbsent(group, g -> new AtomicInteger()).incrementAndGet() > 1) {
                overlaps.incrementAndGet();
            }
            peakGroups.accumulateAndGet(groupsAtOnce.incrementAndGet(), Math::max);
            bothGroups.countDown();
            bothGroups.await(WAIT, TimeUnit.SECONDS);
            Thread.yield();
            groupsAtOnce.decrementAndGet();
            active.get(group).decrementAndGet();
            return Disposition.accept();
        });
        for (int i = 0; i < 4; i++) {
            lanes.inbox.add(delivered("a" + i, "A", 0));
            lanes.inbox.add(delivered("b" + i, "B", 0));
        }
        for (int i = 0; i < 8; i++) {
            lanes.settled();
        }
        assertThat(overlaps.get()).isZero();
        assertThat(peakGroups.get()).isEqualTo(2);
    }

    @Test
    void aSystemThatPinsGroupsIsTrustedToKeepThemApart() throws Exception {
        FakeLanes lanes = new FakeLanes(true);
        CountDownLatch together = new CountDownLatch(2);
        start(options(2, OrderingScope.GROUP, RetryPolicy.system()), lanes, ctx -> {
            together.countDown();
            assertThat(together.await(WAIT, TimeUnit.SECONDS)).isTrue();
            return Disposition.accept();
        });
        lanes.inbox.add(delivered("a1", "A", 0));
        lanes.inbox.add(delivered("a2", "A", 0));
        lanes.settled();
        lanes.settled();
    }

    @Test
    void systemModeReleasesAFailedMessageCounted() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        AtomicInteger calls = new AtomicInteger();
        start(options(1, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(lanes.settled().disposition()).isEqualTo(new Disposition.Release(Optional.empty(), true));
        assertThat(calls).hasValue(1);
        assertThat(sleeper.sleeps).isEmpty();
    }

    @Test
    void managedRetryOnAnOrderedProcessorWaitsInPlaceAndKeepsTheLane() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        start(options(1, OrderingScope.SUBSCRIPTION, RetryPolicy.managed(5, PLAIN)), lanes, ctx -> {
            attempts.add(ctx.attempt());
            if (ctx.attempt() < 3) {
                throw new IllegalStateException("not yet");
            }
            return Disposition.accept();
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(lanes.settled().disposition()).isEqualTo(Disposition.accept());
        assertThat(attempts).containsExactly(1, 2, 3);
        assertThat(sleeper.sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
        assertThat(lanes.settled).isEmpty();
    }

    @Test
    void managedRetryOnAnUnorderedProcessorReleasesWithTheBackoffDelay() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        AtomicInteger calls = new AtomicInteger();
        start(options(2, OrderingScope.NONE, RetryPolicy.managed(3, PLAIN)), lanes, ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        });
        lanes.inbox.add(delivered("m1", null, 1));
        lanes.inbox.add(delivered("m2", null, 2));
        Settled first = lanes.settled();
        Settled second = lanes.settled();
        Map<String, Disposition> byId = Map.of(
                first.message().headers().id().orElseThrow(), first.disposition(),
                second.message().headers().id().orElseThrow(), second.disposition());
        assertThat(byId.get("m1")).isEqualTo(new Disposition.Release(Optional.of(Duration.ofMillis(100)), true));
        assertThat(byId.get("m2")).isEqualTo(new Disposition.Release(Optional.of(Duration.ofMillis(200)), true));
        assertThat(calls).hasValue(2);
        assertThat(sleeper.sleeps).isEmpty();
    }

    @Test
    void afterMaxAttemptsTheMessageIsRejectedToTheSystemsDeadLetter() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        start(options(1, OrderingScope.NONE, RetryPolicy.managed(3, PLAIN)), lanes, ctx -> {
            throw new IllegalStateException("boom");
        });
        lanes.inbox.add(delivered("m1", null, 3));
        Disposition d = lanes.settled().disposition();
        assertThat(d).isInstanceOf(Disposition.Reject.class);
        assertThat(lanes.deadLetter.sent).isEmpty();
    }

    @Test
    void afterMaxAttemptsTheMessageIsCopiedToTheConfiguredDeadLetterAndAccepted() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        SendTarget target = SendTarget.destination(Fakes.QUEUE);
        start(options(1, OrderingScope.GROUP, RetryPolicy.managed(2, PLAIN).withDeadLetter(target)), lanes, ctx -> {
            throw new IllegalStateException("boom");
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(lanes.settled().disposition()).isEqualTo(Disposition.accept());
        assertThat(lanes.deadLetter.sent).hasSize(1);
        OutboundMessage copy = lanes.deadLetter.sent.get(0);
        assertThat(copy.properties())
                .containsEntry("openbroker.dead_letter_attempts", PropertyValue.of(2))
                .containsEntry("openbroker.dead_letter_reason", PropertyValue.of("IllegalStateException"));
        assertThat(sleeper.sleeps).containsExactly(Duration.ofMillis(100));
    }

    @Test
    void anExplicitRejectDeadLettersAtOnceWithoutRetries() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        Disposition reject = new Disposition.Reject(Optional.of("poison"));
        AtomicInteger calls = new AtomicInteger();
        start(options(1, OrderingScope.GROUP, RetryPolicy.managed(5, PLAIN)), lanes, ctx -> {
            calls.incrementAndGet();
            return reject;
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(lanes.settled().disposition()).isEqualTo(reject);
        assertThat(calls).hasValue(1);
        assertThat(sleeper.sleeps).isEmpty();
    }

    @Test
    void stopLetsRunningHandlersFinishWithinTheGrace() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        CountDownLatch inHandler = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        ProcessorEngine engine = start(options(2, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> {
            inHandler.countDown();
            finish.await();
            return Disposition.accept();
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(inHandler.await(WAIT, TimeUnit.SECONDS)).isTrue();
        Thread stopper = Thread.ofVirtual().start(() -> engine.stop(Duration.ofSeconds(WAIT)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT);
        while (stopper.getState() != Thread.State.TIMED_WAITING && stopper.getState() != Thread.State.WAITING) {
            assertThat(System.nanoTime()).isLessThan(deadline);
            Thread.onSpinWait();
        }
        assertThat(lanes.settled).isEmpty();
        finish.countDown();
        stopper.join();
        assertThat(lanes.settled().disposition()).isEqualTo(Disposition.accept());
        assertThat(engine.state()).isEqualTo(LinkState.CLOSED);
        assertThat(lanes.closedLanes).hasValue(2);
    }

    @Test
    void stopInterruptsHandlersPastTheGraceAndReleasesTheirMessagesUncounted() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        CountDownLatch inHandler = new CountDownLatch(1);
        ProcessorEngine engine = start(options(1, OrderingScope.NONE, RetryPolicy.managed(3, PLAIN)), lanes, ctx -> {
            inHandler.countDown();
            new CountDownLatch(1).await();
            return Disposition.accept();
        });
        lanes.inbox.add(delivered("m1", null, 0));
        assertThat(inHandler.await(WAIT, TimeUnit.SECONDS)).isTrue();
        engine.stop(Duration.ofMillis(30));
        assertThat(lanes.settled().disposition()).isEqualTo(new Disposition.Release(Optional.empty(), false));
        assertThat(engine.state()).isEqualTo(LinkState.CLOSED);
        assertThat(lanes.closedLanes).hasValue(1);
    }

    @Test
    void stopReleasesIdleLanesAtOnceAndTakesNothingMore() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        AtomicInteger calls = new AtomicInteger();
        ProcessorEngine engine = start(options(3, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> {
            calls.incrementAndGet();
            return Disposition.accept();
        });
        engine.stop(Duration.ofSeconds(WAIT));
        lanes.inbox.add(delivered("late", null, 0));
        assertThat(lanes.closedLanes).hasValue(3);
        assertThat(lanes.settled).isEmpty();
        assertThat(calls).hasValue(0);
    }

    @Test
    void theObserverScopeWrapsTheHandlerAndEndsWithItsError() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        List<String> trace = new CopyOnWriteArrayList<>();
        MessagingObserver observer = new MessagingObserver() {
            @Override
            public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Observation onReceive(OperationInfo op) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Observation onProcess(OperationInfo op, Message message) {
                trace.add("process");
                return new Observation() {
                    @Override
                    public AutoCloseable scope() {
                        trace.add("open");
                        return () -> trace.add("close");
                    }

                    @Override
                    public void end(Throwable error) {
                        trace.add("end:" + (error == null ? "ok" : error.getMessage()));
                    }
                };
            }

            @Override
            public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
                throw new UnsupportedOperationException();
            }
        };
        start(
                options(1, OrderingScope.NONE, RetryPolicy.system()),
                lanes,
                ctx -> {
                    trace.add("handler");
                    throw new IllegalStateException("bad");
                },
                Fakes.context(clock, sleeper, observer));
        lanes.inbox.add(delivered("m1", null, 0));
        lanes.settled();
        assertThat(trace).containsExactly("process", "open", "handler", "close", "end:bad");
    }

    @Test
    void aJoinedSenderComesFromTheLaneOfTheDelivery() throws Exception {
        FakeLanes lanes = new FakeLanes(false);
        start(options(1, OrderingScope.NONE, RetryPolicy.system()), lanes, ctx -> {
            ctx.sender(SendTarget.destination(Fakes.QUEUE)).send(Fakes.outbound("out"));
            return Disposition.accept();
        });
        lanes.inbox.add(delivered("m1", null, 0));
        lanes.settled();
        assertThat(lanes.deadLetter.sent).hasSize(1);
        assertThat(NODE).isNotNull();
    }
}
