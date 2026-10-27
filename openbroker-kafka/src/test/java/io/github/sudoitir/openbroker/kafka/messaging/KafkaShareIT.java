package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

/** Share groups on a cluster whose {@code share.version} is 2, where the broker dead-letters. */
class KafkaShareIT {

    private final Transport transport = SharedKafka.transport();

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private void send(ResourceId subscription, long seq) {
        try (Sender sender =
                transport.sender(SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults())) {
            sender.send(KafkaMessagingIT.message(seq));
        }
    }

    private static Map<String, String> withDeadLetters(String topic) {
        return Map.of("errors.deadletterqueue.topic.name", "dlq." + topic);
    }

    private static ProcessorOptions one() {
        return ProcessorOptions.defaults().withConcurrency(1);
    }

    @Test
    void theShareKindIsDeclaredWithTheBrokersRetryAndDeadLetters() {
        var kind = transport.capabilities().subscription(Subscriptions.SHARE_GROUP_TOPIC);
        assertThat(kind.orderings()).containsExactly(OrderingScope.NONE);
        assertThat(kind.joinedTransactions()).isFalse();
        assertThat(kind.retry()).isEqualTo(new SystemRetry(false, true, true));
    }

    @Test
    void aReleasedRecordIsDeliveredAgainWithItsDeliveryCountRaised() {
        String topic = SharedKafka.topic("share-release", 1);
        ResourceId group = SharedKafka.shareGroup(topic, withDeadLetters(topic));
        send(group, 1);
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(group, Scope.whole(), one(), ctx -> {
            attempts.add(ctx.attempt());
            return ctx.attempt() == 1 ? new Disposition.Release(Optional.empty(), true) : Disposition.accept();
        })) {
            assertThat(processor.retry().mode()).isEqualTo(RetryMode.SYSTEM);
            await("the second delivery", () -> attempts.size() >= 2);
        }
        assertThat(attempts).containsExactly(1, 2);
    }

    @Test
    void aSlowHandlerKeepsItsRecordBecauseTheLockIsRenewed() {
        String topic = SharedKafka.topic("share-renew", 1);
        ResourceId group = SharedKafka.shareGroup(
                topic,
                Map.of("errors.deadletterqueue.topic.name", "dlq." + topic, "share.record.lock.duration.ms", "2000"));
        send(group, 1);
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(group, Scope.whole(), one(), ctx -> {
            seen.add(ctx.attempt());
            Thread.sleep(7_000);
            return Disposition.accept();
        })) {
            await("the handler to be called", () -> !seen.isEmpty());
            try {
                Thread.sleep(9_000);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        assertThat(seen)
                .as("a lock of 2 s held for 7 s without a second delivery")
                .containsExactly(1);
    }

    @Test
    void recordsWaitingForCreditAndHeldBySlowHandlersAllKeepTheirLocks() {
        String topic = SharedKafka.topic("share-credit", 1);
        ResourceId group = SharedKafka.shareGroup(
                topic,
                Map.of("errors.deadletterqueue.topic.name", "dlq." + topic, "share.record.lock.duration.ms", "2000"));
        int total = 6;
        for (int i = 0; i < total; i++) {
            send(group, i);
        }
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        ProcessorOptions options = new ProcessorOptions(
                ReceiverOptions.consume().withFlow(new FlowControl(2, 0, 0)),
                2,
                OrderingScope.NONE,
                RetryPolicy.defaults(),
                Duration.ofSeconds(30));
        try (Processor processor = transport.process(group, Scope.whole(), options, ctx -> {
            seen.add(KafkaMessagingIT.seq(ctx.message()) + "#" + ctx.attempt());
            Thread.sleep(4_000);
            return Disposition.accept();
        })) {
            await("every record handled", () -> seen.size() >= total);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        assertThat(seen)
                .as("no record's lock ran out while it waited or was handled")
                .hasSize(total);
        assertThat(seen).allSatisfy(entry -> assertThat(entry).endsWith("#1"));
    }

    @Test
    void aRecordReleasedPastTheDeliveryLimitGoesToTheGroupsDeadLetterTopic() {
        String topic = SharedKafka.topic("share-limit", 1);
        SharedKafka.deadLetterTopic("dlq." + topic);
        ResourceId group = SharedKafka.shareGroup(
                topic, Map.of("errors.deadletterqueue.topic.name", "dlq." + topic, "share.delivery.count.limit", "2"));
        send(group, 7);
        List<Integer> attempts = Collections.synchronizedList(new ArrayList<>());
        try (Processor processor = transport.process(group, Scope.whole(), one(), ctx -> {
            attempts.add(ctx.attempt());
            throw new IllegalStateException("never");
        })) {
            await("the dead letter", () -> deadLetters("dlq." + topic) >= 1);
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        assertThat(attempts).containsExactly(1, 2);
        assertThat(deadLetters("dlq." + topic)).isEqualTo(1);
    }

    @Test
    void aSystemPolicyOnAGroupWithNoDeadLetterTopicIsRefusedNamingTheConfigs() {
        String topic = SharedKafka.topic("share-nodlq", 1);
        ResourceId group = SharedKafka.shareGroup(topic, Map.of("share.delivery.count.limit", "3"));
        assertThatThrownBy(() -> transport.process(group, Scope.whole(), one(), ctx -> Disposition.accept()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.getMessage())
                            .contains("errors.deadletterqueue.topic.name")
                            .contains("errors.deadletterqueue.group.enable");
                });
    }

    @Test
    void aManagedPolicyThatOutlastsTheDeliveryLimitIsRefused() {
        String topic = SharedKafka.topic("share-managed", 1);
        ResourceId group = SharedKafka.shareGroup(topic, Map.of("share.delivery.count.limit", "3"));
        ProcessorOptions managed = one().withRetry(RetryPolicy.managed(6, Backoff.defaults()));
        assertThatThrownBy(() -> transport.process(group, Scope.whole(), managed, ctx -> Disposition.accept()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.getMessage()).contains("share.delivery.count.limit");
                });
    }

    /** How many records {@code topic} holds so far, counted from its offsets. */
    private static int deadLetters(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", SharedKafka.cluster().bootstrapServers());
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                return 0;
            }
            var assigned = partitions.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            var begin = consumer.beginningOffsets(assigned);
            return consumer.endOffsets(assigned).entrySet().stream()
                    .mapToInt(e -> (int) (e.getValue() - begin.get(e.getKey())))
                    .sum();
        }
    }
}
