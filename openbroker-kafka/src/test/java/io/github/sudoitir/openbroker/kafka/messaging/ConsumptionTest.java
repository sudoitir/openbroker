package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The poll loop's settlement, credit, rebalance and due-time rules, over a mock consumer. */
class ConsumptionTest {

    private static final String TOPIC = "orders";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final ResourceId SOURCE = ResourceId.of("kafka.group-topic", "g", TOPIC);

    private final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();

    /** A mock that remembers every commit, assigned partition or not. */
    private final MockConsumer<byte[], byte[]> mock = new MockConsumer<>("earliest") {
        @Override
        public synchronized void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            offsets.forEach((partition, offset) -> commits.merge(partition, offset.offset(), Math::max));
            super.commitAsync(offsets, callback);
        }
    };

    private final List<Delivery> delivered = new CopyOnWriteArrayList<>();
    private final List<Collection<TopicPartition>> retracted = new CopyOnWriteArrayList<>();
    private Consumption consumption;

    private final Consuming.Sink sink = new Consuming.Sink() {
        @Override
        public void deliver(Delivery delivery) {
            delivered.add(delivery);
        }

        @Override
        public void retract(Collection<TopicPartition> partitions) {
            retracted.add(partitions);
        }
    };

    @AfterEach
    void stop() {
        if (consumption != null) {
            consumption.close();
        }
    }

    private void open(Guarantee guarantee, int credit, Duration grace) {
        consumption = new Consumption(
                "test",
                new Consumption.Settings(
                        "g", Set.of(TOPIC), SOURCE, guarantee, credit, 0, grace, Consumption.Commits.CONTIGUOUS),
                sink,
                mock);
        mock.updateBeginningOffsets(Map.of(P0, 0L));
        consumption.start();
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, null, ("v" + offset).getBytes(StandardCharsets.UTF_8));
    }

    private void assignAndAdd(int records) {
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(P0));
            for (int i = 0; i < records; i++) {
                mock.addRecord(record(i));
            }
        });
    }

    private long committed() {
        return commits.getOrDefault(P0, -1L);
    }

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void outOfOrderSettlementCommitsOnlyThroughTheContiguousPrefix() {
        open(Guarantee.AT_LEAST_ONCE, 10, Duration.ZERO);
        assignAndAdd(4);
        await("four deliveries", () -> delivered.size() == 4);

        consumption.settle(delivered.get(1));
        consumption.settle(delivered.get(3));
        pause(300);
        assertThat(committed()).as("record 0 is unsettled").isLessThanOrEqualTo(0);

        consumption.settle(delivered.get(0));
        await("commit through record 1", () -> committed() == 2);
        consumption.settle(delivered.get(2));
        await("commit through record 3", () -> committed() == 4);
    }

    @Test
    void creditPausesThePartitionsAndSettlementResumesThem() {
        open(Guarantee.AT_LEAST_ONCE, 2, Duration.ZERO);
        assignAndAdd(5);
        await("two deliveries", () -> delivered.size() == 2);
        await("the partition is paused", () -> mock.paused().contains(P0));
        pause(200);
        assertThat(delivered).as("credit is spent").hasSize(2);

        consumption.settle(delivered.get(0));
        consumption.settle(delivered.get(1));
        await("two more", () -> delivered.size() == 4);
        assertThat(delivered).extracting(Delivery::offset).containsExactly(0L, 1L, 2L, 3L);
    }

    @Test
    void atMostOnceCommitsBeforeAnyRecordIsHandedOut() {
        open(Guarantee.AT_MOST_ONCE, 10, Duration.ZERO);
        assignAndAdd(3);
        await("three deliveries", () -> delivered.size() == 3);
        assertThat(committed()).isEqualTo(3);
    }

    @Test
    void aRevokeLetsARunningHandlerFinishCommitsItsSettlementAndHandsTheRestBack() {
        open(Guarantee.AT_LEAST_ONCE, 10, Duration.ofSeconds(5));
        assignAndAdd(3);
        await("three deliveries", () -> delivered.size() == 3);
        Delivery running = delivered.getFirst();
        assertThat(running.start()).isTrue();

        mock.schedulePollTask(() -> mock.rebalance(List.of()));
        Thread handler = Thread.ofPlatform().start(() -> {
            pause(150);
            consumption.settle(running);
        });
        await("the settled record is committed", () -> committed() == 1);
        assertThat(retracted).isNotEmpty();
        assertThat(delivered.get(1).start())
                .as("a queued record is not taken after the revoke")
                .isFalse();
        assertThatThrownBy(() -> consumption.settle(delivered.get(1)))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.DELIVERY_STALE));
        handler.interrupt();
    }

    @Test
    void aLostPartitionFailsSettlementStaleAndCommitsNothing() {
        open(Guarantee.AT_LEAST_ONCE, 10, Duration.ofSeconds(5));
        assignAndAdd(2);
        await("two deliveries", () -> delivered.size() == 2);
        Delivery running = delivered.getFirst();
        assertThat(running.start()).isTrue();

        mock.schedulePollTask(() -> consumption.rebalances().onPartitionsLost(List.of(P0)));
        await("the partition is dropped", () -> !retracted.isEmpty());

        assertThatThrownBy(() -> consumption.settle(running))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.DELIVERY_STALE));
        assertThat(committed()).isEqualTo(-1);
    }

    @Test
    void aRecordHandedBackIsHandedOutAgainWithItsAttemptRaisedAndHoldsTheCommit() {
        open(Guarantee.AT_LEAST_ONCE, 10, Duration.ZERO);
        assignAndAdd(2);
        await("two deliveries", () -> delivered.size() == 2);

        consumption.settle(delivered.get(1));
        consumption.requeue(delivered.get(0), Duration.ZERO, true);
        await("a third delivery", () -> delivered.size() == 3);
        Delivery again = delivered.get(2);
        assertThat(again.offset()).isZero();
        assertThat(again.attempt()).isEqualTo(2);
        assertThat(again.message().headers().deliveryCount()).hasValue(2);
        pause(200);
        assertThat(committed()).as("the handed-back record holds the commit").isLessThanOrEqualTo(0);

        consumption.settle(again);
        await("commit through both", () -> committed() == 2);
    }

    @Test
    void aRecordWithAFutureDueTimeIsSoughtBackAndItsPartitionPausedUntilDue() {
        open(Guarantee.AT_LEAST_ONCE, 10, Duration.ZERO);
        long due = System.currentTimeMillis() + 800;
        RecordHeaders headers = new RecordHeaders();
        headers.add(Records.DUE, Long.toString(due).getBytes(StandardCharsets.UTF_8));
        headers.add(Records.ATTEMPT, "2".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> retry = new ConsumerRecord<>(
                TOPIC,
                0,
                0,
                0L,
                TimestampType.CREATE_TIME,
                0,
                2,
                null,
                "vv".getBytes(StandardCharsets.UTF_8),
                headers,
                Optional.empty());
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(P0));
            mock.addRecord(retry);
        });
        await("the partition is paused", () -> mock.paused().contains(P0));
        assertThat(delivered).isEmpty();
        assertThat(mock.position(P0)).as("sought back to the record").isZero();
        mock.addRecord(retry);

        await("the record is due", () -> delivered.size() == 1);
        assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(due);
        assertThat(delivered.getFirst().attempt()).isEqualTo(3);
        assertThat(new ArrayList<>(mock.paused())).doesNotContain(P0);
    }
}
