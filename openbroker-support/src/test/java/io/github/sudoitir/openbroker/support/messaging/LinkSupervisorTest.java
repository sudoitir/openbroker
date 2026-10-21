package io.github.sudoitir.openbroker.support.messaging;

import static io.github.sudoitir.openbroker.support.messaging.Fakes.NODE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Recovery;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeClock;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeSleeper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LinkSupervisorTest {

    private static final Backoff PLAIN = new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(10), 0.0);
    private static final NodeId BACKUP = NodeId.of("n2");

    private final FakeClock clock = new FakeClock();
    private final FakeSleeper sleeper = new FakeSleeper(clock);
    private final List<CallInfo> admitted = new CopyOnWriteArrayList<>();
    private final List<Outcome> outcomes = new CopyOnWriteArrayList<>();
    private final LinkedBlockingQueue<LinkEvent> events = new LinkedBlockingQueue<>();

    private EngineContext context() {
        CallInterceptor interceptor = call -> {
            admitted.add(call);
            return new Permit() {
                @Override
                public void complete(Outcome outcome, Optional<Instant> serverTime) {
                    outcomes.add(outcome);
                }
            };
        };
        return new EngineContext(clock, new Random(1), sleeper, task -> task, MessagingObserver.none(), interceptor);
    }

    private static BrokerException unreachable() {
        return new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, NODE, "down");
    }

    private LinkEvent event() throws InterruptedException {
        LinkEvent e = events.poll(5, TimeUnit.SECONDS);
        assertThat(e).as("an event within 5 s").isNotNull();
        return e;
    }

    /** A link whose first {@code failures} reopen attempts fail. */
    private static final class FlakyLink implements LinkSupervisor.Link {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicBoolean suspended = new AtomicBoolean();
        final AtomicBoolean failed = new AtomicBoolean();
        final int failures;

        FlakyLink(int failures) {
            this.failures = failures;
        }

        @Override
        public NodeId reopen() {
            if (attempts.incrementAndGet() <= failures) {
                throw unreachable();
            }
            return BACKUP;
        }

        @Override
        public void suspended(BrokerException cause) {
            suspended.set(true);
        }

        @Override
        public void failed(BrokerException cause) {
            failed.set(true);
        }
    }

    @Test
    void recoversWithBackoffAndTellsWhoServes() throws Exception {
        FlakyLink link = new FlakyLink(2);
        LinkSupervisor supervisor = new LinkSupervisor(
                "link-1", NODE, "tcp://n1:61616", new Recovery(PLAIN, Optional.empty()), link, context());
        supervisor.listen(events::add);
        BrokerException cause = unreachable();

        supervisor.lost(cause);

        LinkEvent recovering = event();
        assertThat(recovering.state()).isEqualTo(LinkState.RECOVERING);
        assertThat(recovering.cause()).containsSame(cause);
        assertThat(recovering.link()).isEqualTo("link-1");
        LinkEvent open = event();
        assertThat(open.state()).isEqualTo(LinkState.OPEN);
        assertThat(open.servedBy()).contains(BACKUP);
        assertThat(open.node()).isEqualTo(NODE);
        assertThat(supervisor.state()).isEqualTo(LinkState.OPEN);
        assertThat(sleeper.sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
        assertThat(link.suspended).isTrue();
        assertThat(link.failed).isFalse();
    }

    @Test
    void everyAttemptIsADataPlaneCallTheInterceptorAdmits() throws Exception {
        LinkSupervisor supervisor = new LinkSupervisor(
                "link-1", NODE, "tcp://n1:61616", new Recovery(PLAIN, Optional.empty()), new FlakyLink(1), context());
        supervisor.listen(events::add);
        supervisor.lost(unreachable());
        event();
        event();
        assertThat(admitted).hasSize(2).allSatisfy(call -> {
            assertThat(call.operation()).isEqualTo("link.recover");
            assertThat(call.plane()).isEqualTo(Plane.DATA);
            assertThat(call.node()).isEqualTo(NODE);
            assertThat(call.endpoint()).isEqualTo("tcp://n1:61616");
        });
        assertThat(outcomes).extracting(Outcome::success).containsExactly(false, true);
    }

    @Test
    void aRefusedAttemptCountsAsAFailedOne() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CallInterceptor refuseOnce = call -> {
            if (calls.incrementAndGet() == 1) {
                throw new BrokerException(
                        Status.RESOURCE_EXHAUSTED,
                        ErrorInfo.of(ErrorReasons.RATE_LIMITED),
                        Dispatch.NOT_SENT,
                        NODE,
                        "slow down");
            }
            return Permit.unlimited();
        };
        EngineContext refusing =
                new EngineContext(clock, new Random(1), sleeper, task -> task, MessagingObserver.none(), refuseOnce);
        FlakyLink link = new FlakyLink(0);
        LinkSupervisor supervisor =
                new LinkSupervisor("l", NODE, "e", new Recovery(PLAIN, Optional.empty()), link, refusing);
        supervisor.listen(events::add);
        supervisor.lost(unreachable());
        event();
        assertThat(event().state()).isEqualTo(LinkState.OPEN);
        assertThat(link.attempts).hasValue(1);
        assertThat(sleeper.sleeps).containsExactly(Duration.ofMillis(100));
    }

    @Test
    void givesUpAfterTheLimitWithTheLastCause() throws Exception {
        FlakyLink link = new FlakyLink(Integer.MAX_VALUE);
        Backoff backoff = new Backoff(Duration.ofMillis(400), 2.0, Duration.ofSeconds(10), 0.0);
        LinkSupervisor supervisor = new LinkSupervisor(
                "l", NODE, "e", new Recovery(backoff, Optional.of(Duration.ofSeconds(1))), link, context());
        supervisor.listen(events::add);
        supervisor.lost(unreachable());
        assertThat(event().state()).isEqualTo(LinkState.RECOVERING);
        LinkEvent failed = event();
        assertThat(failed.state()).isEqualTo(LinkState.FAILED);
        assertThat(failed.cause()).isPresent();
        assertThat(failed.cause().get().status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(supervisor.state()).isEqualTo(LinkState.FAILED);
        assertThat(link.failed).isTrue();
        // attempt 1 at 0 ms, waits 400 ms, attempt 2 at 400 ms, waits the 600 ms left, then gives up
        assertThat(sleeper.sleeps).containsExactly(Duration.ofMillis(400), Duration.ofMillis(600));
        assertThat(link.attempts).hasValue(2);
        supervisor.lost(unreachable());
        assertThat(supervisor.state()).isEqualTo(LinkState.FAILED);
    }

    @Test
    void aSecondLossWhileRecoveringStartsNothingNew() throws Exception {
        AtomicReference<LinkSupervisor> self = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger();
        LinkSupervisor.Link link = new LinkSupervisor.Link() {
            @Override
            public NodeId reopen() {
                attempts.incrementAndGet();
                return BACKUP;
            }

            @Override
            public void suspended(BrokerException cause) {
                self.get().lost(unreachable());
            }
        };
        LinkSupervisor supervisor =
                new LinkSupervisor("l", NODE, "e", new Recovery(PLAIN, Optional.empty()), link, context());
        self.set(supervisor);
        supervisor.listen(events::add);
        supervisor.lost(unreachable());
        assertThat(event().state()).isEqualTo(LinkState.RECOVERING);
        assertThat(event().state()).isEqualTo(LinkState.OPEN);
        supervisor.close();
        assertThat(event().state()).isEqualTo(LinkState.CLOSED);
        assertThat(events).isEmpty();
        assertThat(attempts).hasValue(1);
    }

    @Test
    void eventsReachListenersInOrderOnVirtualThreads() throws Exception {
        List<Boolean> virtual = new CopyOnWriteArrayList<>();
        LinkSupervisor supervisor =
                new LinkSupervisor("l", NODE, "e", new Recovery(PLAIN, Optional.empty()), new FlakyLink(0), context());
        supervisor.listen(e -> {
            virtual.add(Thread.currentThread().isVirtual());
            events.add(e);
        });
        for (int i = 0; i < 3; i++) {
            supervisor.lost(unreachable());
            assertThat(event().state()).isEqualTo(LinkState.RECOVERING);
            assertThat(event().state()).isEqualTo(LinkState.OPEN);
        }
        supervisor.close();
        assertThat(event().state()).isEqualTo(LinkState.CLOSED);
        assertThat(virtual).hasSize(7).containsOnly(true);
    }

    @Test
    void closingStopsARecoveryInProgress() throws Exception {
        LinkEvent last;
        FlakyLink link = new FlakyLink(Integer.MAX_VALUE);
        EngineContext real = new EngineContext(
                clock, new Random(1), Sleeper.system(), task -> task, MessagingObserver.none(), CallInterceptor.none());
        LinkSupervisor supervisor = new LinkSupervisor(
                "l",
                NODE,
                "e",
                new Recovery(new Backoff(Duration.ofSeconds(30), 1.0, Duration.ofSeconds(30), 0.0), Optional.empty()),
                link,
                real);
        supervisor.listen(events::add);
        supervisor.lost(unreachable());
        event();
        supervisor.close();
        last = event();
        assertThat(last.state()).isEqualTo(LinkState.CLOSED);
        assertThat(supervisor.state()).isEqualTo(LinkState.CLOSED);
    }
}
