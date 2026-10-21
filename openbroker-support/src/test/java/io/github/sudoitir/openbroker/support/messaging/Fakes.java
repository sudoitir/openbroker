package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/** Test doubles shared by the engine tests. */
final class Fakes {

    static final NodeId NODE = NodeId.of("n1");
    static final ResourceId QUEUE = ResourceId.of("fake.queue", "orders");

    private Fakes() {}

    /** A clock that moves only when told to. */
    static final class FakeClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration d) {
            now.updateAndGet(i -> i.plus(d));
        }
    }

    /** A sleeper that returns at once, moves the clock and records what was asked. */
    static final class FakeSleeper implements Sleeper {
        final List<Duration> sleeps = new CopyOnWriteArrayList<>();
        private final FakeClock clock;

        FakeSleeper(FakeClock clock) {
            this.clock = clock;
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            sleeps.add(duration);
            clock.advance(duration);
        }
    }

    static EngineContext context(FakeClock clock, Sleeper sleeper) {
        return new EngineContext(
                clock, new Random(7), sleeper, task -> task, MessagingObserver.none(), CallInterceptor.none());
    }

    static EngineContext context(FakeClock clock, Sleeper sleeper, MessagingObserver observer) {
        return new EngineContext(clock, new Random(7), sleeper, task -> task, observer, CallInterceptor.none());
    }

    static OperationInfo info(OperationType type) {
        return new OperationInfo(
                "fake",
                type,
                type.name().toLowerCase(),
                QUEUE,
                Optional.empty(),
                NODE,
                Optional.empty(),
                OptionalInt.empty(),
                1);
    }

    static OutboundMessage outbound(String body) {
        return OutboundMessage.of(MessageBody.of(body.getBytes(StandardCharsets.UTF_8)));
    }

    static OutboundMessage outbound(int bytes) {
        return OutboundMessage.of(MessageBody.of(new byte[bytes]));
    }

    static Message delivered(String position, String group, int deliveryCount) {
        MessageHeaders.Builder headers = MessageHeaders.builder().id(position);
        if (group != null) {
            headers.groupId(group);
        }
        if (deliveryCount > 0) {
            headers.deliveryCount(deliveryCount);
        }
        return new Message(
                new MessageRef(QUEUE, Shard.node(NODE), position),
                headers.build(),
                Map.of(),
                MessageBody.of(position.getBytes(StandardCharsets.UTF_8)),
                Set.of(),
                Map.of());
    }

    static SendReceipt receipt(String id) {
        return new SendReceipt(Optional.of(id), Optional.empty(), Optional.of(NODE), false);
    }

    /** A sender that keeps what it is sent. */
    static final class RecordingSender implements Sender {
        final List<OutboundMessage> sent = new ArrayList<>();

        @Override
        public String id() {
            return "dead-letter";
        }

        @Override
        public LinkState state() {
            return LinkState.OPEN;
        }

        @Override
        public Optional<DeduplicationWindow> deduplication() {
            return Optional.empty();
        }

        @Override
        public synchronized SendReceipt send(OutboundMessage message) {
            sent.add(message);
            return SendReceipt.accepted();
        }

        @Override
        public Offer offer(OutboundMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush(Duration timeout) {}

        @Override
        public void commit() {}

        @Override
        public void rollback() {}

        @Override
        public void close() {}
    }
}
