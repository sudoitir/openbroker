package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/** The consumer-group messaging client against a real three-broker cluster. */
class KafkaMessagingIT {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private final Transport transport = SharedKafka.transport();

    static OutboundMessage message(long seq) {
        return new OutboundMessage(
                MessageHeaders.empty(),
                Map.of("seq", PropertyValue.of(seq)),
                MessageBody.of(("m" + seq).getBytes(StandardCharsets.UTF_8)),
                Map.of());
    }

    static long seq(Message message) {
        return Long.parseLong(message.properties().get("seq").asText());
    }

    private static List<Message> receive(Receiver receiver, int count) {
        long until = System.nanoTime() + WAIT.toNanos();
        List<Message> got = new ArrayList<>();
        while (got.size() < count && System.nanoTime() < until) {
            got.addAll(receiver.receive(count - got.size(), Duration.ofMillis(500)));
        }
        return got;
    }

    private void send(ResourceId target, int count) {
        try (Sender sender =
                transport.sender(SendTarget.subscription(target), Scope.whole(), SenderOptions.defaults())) {
            for (int i = 0; i < count; i++) {
                sender.send(message(i));
            }
        }
    }

    @Test
    void sendsAndReceivesAndSettlementCommits() {
        String topic = SharedKafka.topic("basic", 1);
        ResourceId group = SharedKafka.group(topic);
        send(group, 5);
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            List<Message> got = receive(receiver, 5);
            assertThat(got).extracting(KafkaMessagingIT::seq).containsExactly(0L, 1L, 2L, 3L, 4L);
            got.forEach(m -> receiver.settle(m, Disposition.accept()));
        }
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3))).isEmpty();
        }
    }

    @Test
    void unsettledRecordsAreDeliveredAgainAfterClose() {
        String topic = SharedKafka.topic("redeliver", 1);
        ResourceId group = SharedKafka.group(topic);
        send(group, 3);
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receive(receiver, 3)).hasSize(3);
        }
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receive(receiver, 3)).extracting(KafkaMessagingIT::seq).containsExactly(0L, 1L, 2L);
        }
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition) {
        long until = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    @Test
    void aReceiverNeverHoldsMoreThanItsCreditAndTheRestWaits() {
        String topic = SharedKafka.topic("credit", 3);
        ResourceId group = SharedKafka.group(topic);
        send(group, 12);
        ReceiverOptions options = ReceiverOptions.consume().withFlow(new FlowControl(3, 0, 0));
        try (Receiver receiver = transport.receiver(group, Scope.whole(), options)) {
            List<Message> first = receive(receiver, 3);
            assertThat(first).hasSize(3);
            assertThat(receiver.receive(10, Duration.ofSeconds(2)))
                    .as("credit is spent")
                    .isEmpty();
            List<Message> seen = new ArrayList<>(first);
            List<Message> window = first;
            while (seen.size() < 12) {
                window.forEach(m -> receiver.settle(m, Disposition.accept()));
                window = receive(receiver, Math.min(3, 12 - seen.size()));
                assertThat(window).isNotEmpty().hasSizeLessThanOrEqualTo(3);
                seen.addAll(window);
            }
            window.forEach(m -> receiver.settle(m, Disposition.accept()));
            assertThat(seen).extracting(KafkaMessagingIT::seq).doesNotHaveDuplicates();
        }
    }

    @Test
    void releasingOneRecordHandsBackOnlyThatRecordWithItsAttemptRaised() {
        String topic = SharedKafka.topic("release", 1);
        ResourceId group = SharedKafka.group(topic);
        send(group, 3);
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            List<Message> got = receive(receiver, 3);
            receiver.settle(got.get(0), Disposition.accept());
            receiver.settle(got.get(1), new Disposition.Release(Optional.empty(), true));
            receiver.settle(got.get(2), Disposition.accept());
            List<Message> again = receive(receiver, 1);
            assertThat(again).extracting(KafkaMessagingIT::seq).containsExactly(1L);
            assertThat(again.getFirst().headers().deliveryCount()).hasValue(2);
            assertThat(receiver.receive(1, Duration.ofSeconds(1))).isEmpty();
            receiver.settle(again.getFirst(), Disposition.accept());
        }
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3)))
                    .as("everything is committed")
                    .isEmpty();
        }
    }

    @Test
    void aReleasedRecordWithADelayIsHiddenUntilDue() {
        String topic = SharedKafka.topic("delay", 1);
        ResourceId group = SharedKafka.group(topic);
        send(group, 1);
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            Message first = receive(receiver, 1).getFirst();
            long released = System.nanoTime();
            receiver.settle(first, new Disposition.Release(Optional.of(Duration.ofSeconds(2)), true));
            assertThat(receiver.receive(1, Duration.ofMillis(800))).isEmpty();
            assertThat(receive(receiver, 1)).hasSize(1);
            assertThat(Duration.ofNanos(System.nanoTime() - released)).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
        }
    }

    @Test
    void aProcessorKeepsEachGroupInOrderAndRunsDifferentGroupsAtOnce() {
        String topic = SharedKafka.topic("group-order", 3);
        ResourceId group = SharedKafka.group(topic);
        int groups = 3;
        int perGroup = 60;
        try (Sender sender = transport.sender(
                SendTarget.subscription(group),
                Scope.whole(),
                SenderOptions.defaults()
                        .withDeduplication(DeduplicationScope.SESSION)
                        .withOrdering(OrderingScope.GROUP))) {
            List<CompletionStage<?>> receipts = new ArrayList<>();
            for (int n = 0; n < perGroup; n++) {
                for (int g = 0; g < groups; g++) {
                    OutboundMessage m = message(n)
                            .withHeaders(
                                    MessageHeaders.builder().groupId("g" + g).build());
                    receipts.add(((Offer.Accepted) sender.offer(m)).receipt());
                }
            }
            receipts.forEach(r -> r.toCompletableFuture().join());
        }
        Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
        Map<String, List<Long>> order = new ConcurrentHashMap<>();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        try (Processor processor = transport.process(
                group,
                Scope.whole(),
                ProcessorOptions.defaults().withOrdering(OrderingScope.GROUP).withConcurrency(4),
                ctx -> {
                    String key = ctx.message().headers().groupId().orElse("none");
                    AtomicInteger inGroup = running.computeIfAbsent(key, k -> new AtomicInteger());
                    if (inGroup.incrementAndGet() > 1) {
                        overlaps.incrementAndGet();
                    }
                    try {
                        order.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(seq(ctx.message()));
                        Thread.sleep(1);
                    } finally {
                        inGroup.decrementAndGet();
                    }
                    handled.incrementAndGet();
                    return Disposition.accept();
                })) {
            await("every message handled", Duration.ofSeconds(60), () -> handled.get() >= groups * perGroup);
        }
        assertThat(overlaps).hasValue(0);
        for (int g = 0; g < groups; g++) {
            assertThat(order.get("g" + g))
                    .containsExactlyElementsOf(
                            LongStream.range(0, perGroup).boxed().toList());
        }
        try (Receiver receiver = transport.receiver(group, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3)))
                    .as("all settled and committed")
                    .isEmpty();
        }
    }

    @Test
    void orderingTheWholeSubscriptionIsRefusedOnATopicOfSeveralPartitions() {
        String topic = SharedKafka.topic("whole", 3);
        ResourceId group = SharedKafka.group(topic);
        assertThatThrownBy(() -> transport.process(
                        group,
                        Scope.whole(),
                        ProcessorOptions.defaults().withConcurrency(1).withOrdering(OrderingScope.SUBSCRIPTION),
                        ctx -> Disposition.accept()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                    assertThat(e.getMessage()).contains(topic).contains("3 partitions");
                });
    }

    @Test
    void aGraceTimeLongerThanTheRebalanceTimeoutIsRefusedNamingBoth() {
        String topic = SharedKafka.topic("grace", 1);
        ResourceId group = SharedKafka.group(topic);
        ProcessorOptions options = new ProcessorOptions(
                ReceiverOptions.consume(), 1, OrderingScope.NONE, RetryPolicy.defaults(), Duration.ofMinutes(2));
        assertThatThrownBy(() -> transport.process(group, Scope.whole(), options, ctx -> Disposition.accept()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                    assertThat(e.getMessage()).contains("PT2M").contains("PT1M");
                });
    }

    @Test
    void aTransactedReceiverCommitsAndRollsBackTogetherWithWhatItSent() {
        String from = SharedKafka.topic("tx-from", 1);
        String to = SharedKafka.topic("tx-to", 1);
        ResourceId source = SharedKafka.group(from);
        ResourceId target = SharedKafka.group(to);
        send(source, 4);
        ReceiverOptions transacted = ReceiverOptions.consume().withGuarantee(Guarantee.TRANSACTED);
        try (Receiver receiver = transport.receiver(source, Scope.whole(), transacted)) {
            Sender joined = receiver.sender(SendTarget.subscription(target));
            for (int attempt = 1; attempt <= 2; attempt++) {
                List<Message> got = receive(receiver, 4);
                assertThat(got).extracting(KafkaMessagingIT::seq).containsExactlyInAnyOrder(0L, 1L, 2L, 3L);
                for (Message m : got) {
                    joined.send(OutboundMessage.copyOf(m));
                    receiver.settle(m, Disposition.accept());
                }
                if (attempt == 1) {
                    receiver.rollback();
                    try (Receiver reader = transport.receiver(target, Scope.whole(), ReceiverOptions.consume())) {
                        assertThat(reader.receive(5, Duration.ofSeconds(3)))
                                .as("an aborted send is invisible")
                                .isEmpty();
                    }
                } else {
                    receiver.commit();
                }
            }
        }
        try (Receiver reader = transport.receiver(target, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receive(reader, 4)).extracting(KafkaMessagingIT::seq).containsExactlyInAnyOrder(0L, 1L, 2L, 3L);
        }
        try (Receiver again = transport.receiver(source, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(again.receive(5, Duration.ofSeconds(3)))
                    .as("the input is committed")
                    .isEmpty();
        }
    }

    @Test
    void aTransactedProcessorMovesEveryRecordOnceAndAnAbortedHandlerLeavesNoTrace() {
        String from = SharedKafka.topic("txp-from", 2);
        String to = SharedKafka.topic("txp-to", 1);
        ResourceId source = SharedKafka.group(from);
        ResourceId target = SharedKafka.group(to);
        send(source, 10);
        AtomicInteger calls = new AtomicInteger();
        Map<Long, AtomicInteger> failedOnce = new ConcurrentHashMap<>();
        try (Processor processor = transport.process(
                source,
                Scope.whole(),
                new ProcessorOptions(
                        ReceiverOptions.consume().withGuarantee(Guarantee.TRANSACTED),
                        2,
                        OrderingScope.NONE,
                        RetryPolicy.managed(3, Backoff.defaults()),
                        Duration.ofSeconds(30)),
                ctx -> {
                    calls.incrementAndGet();
                    ctx.sender(SendTarget.subscription(target)).send(OutboundMessage.copyOf(ctx.message()));
                    if (seq(ctx.message()) == 3
                            && failedOnce
                                            .computeIfAbsent(3L, k -> new AtomicInteger())
                                            .getAndIncrement()
                                    == 0) {
                        throw new IllegalStateException("abort the first attempt");
                    }
                    return Disposition.accept();
                })) {
            try (Receiver reader = transport.receiver(target, Scope.whole(), ReceiverOptions.consume())) {
                List<Message> moved = receive(reader, 10);
                assertThat(moved)
                        .extracting(KafkaMessagingIT::seq)
                        .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
                assertThat(reader.receive(10, Duration.ofSeconds(2)))
                        .as("the aborted attempt left no copy")
                        .isEmpty();
            }
        }
        assertThat(calls.get()).isEqualTo(11);
    }

    @Test
    void aMemberThatJoinsTakesOverPartitionsAndNoSettledRecordIsHandledAgain() {
        String topic = SharedKafka.topic("join", 4);
        ResourceId group = SharedKafka.group(topic);
        int total = 400;
        try (Sender sender =
                transport.sender(SendTarget.subscription(group), Scope.whole(), SenderOptions.defaults())) {
            for (int i = 0; i < total; i++) {
                sender.send(message(i)
                        .withHeaders(
                                MessageHeaders.builder().groupId("k" + i % 64).build()));
            }
        }
        Map<Long, AtomicInteger> handled = new ConcurrentHashMap<>();
        AtomicInteger byFirst = new AtomicInteger();
        AtomicInteger bySecond = new AtomicInteger();
        ProcessorOptions options = ProcessorOptions.defaults().withConcurrency(1);
        try (Processor first = transport.process(group, Scope.whole(), options, ctx -> {
            handled.computeIfAbsent(seq(ctx.message()), k -> new AtomicInteger())
                    .incrementAndGet();
            byFirst.incrementAndGet();
            Thread.sleep(25);
            return Disposition.accept();
        })) {
            await("the first member is working", Duration.ofSeconds(30), () -> handled.size() >= 10);
            try (Processor second = transport.process(group, Scope.whole(), options, ctx -> {
                handled.computeIfAbsent(seq(ctx.message()), k -> new AtomicInteger())
                        .incrementAndGet();
                bySecond.incrementAndGet();
                Thread.sleep(25);
                return Disposition.accept();
            })) {
                await("every record handled", Duration.ofSeconds(90), () -> handled.size() >= total);
            }
        }
        assertThat(bySecond.get())
                .as("the second member took over partitions (first " + byFirst + ")")
                .isPositive();
        assertThat(handled.values()).allSatisfy(count -> assertThat(count).hasValue(1));
    }
}
