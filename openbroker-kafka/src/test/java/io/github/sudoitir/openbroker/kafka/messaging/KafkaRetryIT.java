package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/** Retries and dead letters of consumer-group processors, against a real three-broker cluster. */
class KafkaRetryIT {

    private static final Backoff ONE_SECOND = new Backoff(Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1), 0);
    private static final Backoff QUICK = new Backoff(Duration.ofMillis(50), 1.0, Duration.ofMillis(50), 0);

    private final Transport transport = SharedKafka.transport();

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
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

    private void send(ResourceId target, int count, MessageHeaders headers) {
        try (Sender sender =
                transport.sender(SendTarget.subscription(target), Scope.whole(), SenderOptions.defaults())) {
            for (int i = 0; i < count; i++) {
                sender.send(KafkaMessagingIT.message(i).withHeaders(headers));
            }
        }
    }

    private static ProcessorOptions options(RetryPolicy retry, OrderingScope ordering, Guarantee guarantee) {
        return new ProcessorOptions(
                ReceiverOptions.consume().withGuarantee(guarantee), 1, ordering, retry, Duration.ofSeconds(30));
    }

    private List<Message> deadLetters(String topic, int count) {
        ResourceId dead = SharedKafka.group(topic + ".dlt");
        List<Message> got = new ArrayList<>();
        try (Receiver receiver = transport.receiver(dead, Scope.whole(), ReceiverOptions.consume())) {
            await("the dead letters", () -> {
                got.addAll(receiver.receive(count, Duration.ofMillis(300)));
                return got.size() >= count;
            });
            got.forEach(m -> receiver.settle(m, Disposition.accept()));
        }
        return got;
    }

    @Test
    void theDefaultPolicyRetriesInPlaceAndCreatesNoRetryTopic() {
        String topic = SharedKafka.topic("default-retry", 1);
        ResourceId subscription = SharedKafka.group(topic);
        send(subscription, 1, MessageHeaders.empty());
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(
                subscription, Scope.whole(), ProcessorOptions.defaults().withConcurrency(1), ctx -> {
                    attempts.add(ctx.attempt());
                    return ctx.attempt() < 3 ? failure() : Disposition.accept();
                })) {
            assertThat(processor.retry().mode()).isEqualTo(RetryMode.MANAGED);
            assertThat(processor.retry().maxAttempts()).isEqualTo(10);
            await("the third attempt", () -> attempts.size() >= 3);
            assertThat(attempts).containsExactly(1, 2, 3);
        }
        assertThat(SharedKafka.topics(topic)).containsExactlyInAnyOrder(topic, topic + ".dlt");
    }

    private static Disposition failure() {
        throw new IllegalStateException("not yet");
    }

    @Test
    void anOrderedManagedProcessorRetriesInPlaceAndKeepsTheOrder() {
        String topic = SharedKafka.topic("ordered-retry", 1);
        ResourceId subscription = SharedKafka.group(topic);
        send(subscription, 3, MessageHeaders.builder().groupId("g").build());
        List<Long> handled = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(
                subscription,
                Scope.whole(),
                options(RetryPolicy.managed(3, QUICK), OrderingScope.GROUP, Guarantee.AT_LEAST_ONCE),
                ctx -> {
                    handled.add(KafkaMessagingIT.seq(ctx.message()));
                    return KafkaMessagingIT.seq(ctx.message()) == 0 && ctx.attempt() == 1
                            ? failure()
                            : Disposition.accept();
                })) {
            await("every record handled", () -> handled.size() >= 4);
        }
        assertThat(handled).containsExactly(0L, 0L, 1L, 2L);
        assertThat(SharedKafka.topics(topic + ".retry"))
                .as("an ordered policy needs no retry topic")
                .isEmpty();
    }

    @Test
    void anUnorderedManagedFailureWaitsInARetryTopicWhileThePartitionMoves() {
        String topic = SharedKafka.topic("topic-retry", 1);
        ResourceId subscription = SharedKafka.group(topic);
        send(subscription, 2, MessageHeaders.empty());
        List<String> handled = Collections.synchronizedList(new ArrayList<>());
        long[] retriedAt = new long[1];
        long started = System.nanoTime();
        try (Processor processor = transport.process(
                subscription,
                Scope.whole(),
                options(RetryPolicy.managed(3, ONE_SECOND), OrderingScope.NONE, Guarantee.AT_LEAST_ONCE),
                ctx -> {
                    long seq = KafkaMessagingIT.seq(ctx.message());
                    handled.add(seq + "#" + ctx.attempt());
                    if (seq == 0 && ctx.attempt() == 1) {
                        return failure();
                    }
                    if (seq == 0) {
                        retriedAt[0] = System.nanoTime();
                    }
                    return Disposition.accept();
                })) {
            await("the retry", () -> handled.contains("0#2"));
        }
        assertThat(handled).containsExactly("0#1", "1#1", "0#2");
        assertThat(SharedKafka.topics(topic + ".retry-"))
                .containsExactlyInAnyOrder(topic + ".retry-1", topic + ".retry-2");
        assertThat(Duration.ofNanos(retriedAt[0] - started)).isGreaterThanOrEqualTo(Duration.ofSeconds(1));

        try (Receiver receiver = transport.receiver(subscription, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3)))
                    .as("everything is settled and committed")
                    .isEmpty();
        }
    }

    @Test
    void aRecordThatKeepsFailingIsDeadLetteredWithItsOrigin() {
        String topic = SharedKafka.topic("dead", 1);
        ResourceId subscription = SharedKafka.group(topic);
        send(subscription, 2, MessageHeaders.empty());
        AtomicInteger poisoned = new AtomicInteger();
        List<Long> handled = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(
                subscription,
                Scope.whole(),
                options(RetryPolicy.managed(2, QUICK), OrderingScope.NONE, Guarantee.AT_LEAST_ONCE),
                ctx -> {
                    if (KafkaMessagingIT.seq(ctx.message()) == 1) {
                        poisoned.incrementAndGet();
                        return failure();
                    }
                    handled.add(KafkaMessagingIT.seq(ctx.message()));
                    return Disposition.accept();
                })) {
            List<Message> dead = deadLetters(topic, 1);
            Message letter = dead.getFirst();
            assertThat(KafkaMessagingIT.seq(letter)).isEqualTo(1);
            var origin = letter.extension(Records.EXTENSION);
            assertThat(origin.raw("origin_topic")).hasValue(topic);
            assertThat(origin.raw("origin_partition")).hasValue("0");
            assertThat(origin.raw("origin_offset")).hasValue("1");
            assertThat(origin.raw("origin_attempts")).hasValue("2");
            assertThat(origin.raw("origin_reason")).hasValue("The handler failed 2 times");
            assertThat(poisoned).hasValue(2);
        }
        assertThat(handled).containsExactly(0L);
        try (Receiver receiver = transport.receiver(subscription, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3)))
                    .as("the dead letter is settled")
                    .isEmpty();
        }
    }

    @Test
    void aRejectionDeadLettersAtOnceWithItsReasonInTheSameTransactionAsTheCommit() {
        String topic = SharedKafka.topic("reject", 1);
        ResourceId subscription = SharedKafka.group(topic);
        send(subscription, 2, MessageHeaders.empty());
        try (Processor processor = transport.process(
                subscription,
                Scope.whole(),
                options(RetryPolicy.managed(3, QUICK), OrderingScope.NONE, Guarantee.TRANSACTED),
                ctx -> KafkaMessagingIT.seq(ctx.message()) == 0
                        ? new Disposition.Reject(Optional.of("bad input"))
                        : Disposition.accept())) {
            List<Message> dead = deadLetters(topic, 1);
            assertThat(KafkaMessagingIT.seq(dead.getFirst())).isZero();
            assertThat(dead.getFirst().extension(Records.EXTENSION).raw("origin_reason"))
                    .hasValue("bad input");
        }
        try (Receiver receiver = transport.receiver(subscription, Scope.whole(), ReceiverOptions.consume())) {
            assertThat(receiver.receive(5, Duration.ofSeconds(3)))
                    .as("the input is committed")
                    .isEmpty();
        }
    }
}
