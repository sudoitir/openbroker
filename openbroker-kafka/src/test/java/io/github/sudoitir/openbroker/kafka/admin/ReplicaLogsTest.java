package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

/** Replica bytes and lag, including a broker that does not answer describeLogDirs. */
class ReplicaLogsTest {

    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private static Await await() {
        Clock clock = Clock.fixed(Instant.parse("2026-11-18T12:00:00Z"), ZoneOffset.UTC);
        return new Await(Deadline.after(Duration.ofSeconds(5), clock), clock, NodeId.of("cluster"));
    }

    @Test
    void aReplicaLagOf40IsItsLagAndItsBytes() {
        Attributes attributes = ReplicaLogs.attributes(new ReplicaInfo(8192, 40, false));

        assertThat(attributes.raw(KafkaKinds.OFFSET_LAG)).contains(40L);
        assertThat(attributes.raw(KafkaKinds.REPLICA_SIZE_BYTES)).contains(8192L);
        assertThat(attributes.raw(KafkaKinds.FUTURE_REPLICA)).isEmpty();
    }

    @Test
    void aFutureReplicaIsFlaggedAndDoesNotReplaceTheCurrentLog() {
        Map<String, LogDirDescription> dirs = Map.of(
                "/current", new LogDirDescription(null, Map.of(ORDERS, new ReplicaInfo(100, 1, false))),
                "/future", new LogDirDescription(null, Map.of(ORDERS, new ReplicaInfo(5, 40, true))));

        ReplicaLogs.Read read = ReplicaLogs.read(await(), Map.of(1, KafkaFuture.completedFuture(dirs)));

        assertThat(read.current(1, ORDERS).size()).isEqualTo(100);
        assertThat(read.current(1, ORDERS).offsetLag()).isEqualTo(1);
        assertThat(read.future(1, ORDERS).isFuture()).isTrue();
        assertThat(ReplicaLogs.attributes(read.future(1, ORDERS)).raw(KafkaKinds.FUTURE_REPLICA))
                .contains(true);
        assertThat(ReplicaLogs.attributes(read.future(1, ORDERS)).raw(KafkaKinds.OFFSET_LAG))
                .contains(40L);
        assertThat(ReplicaLogs.leaderSize(read, new Node(1, "one", 9092), ORDERS))
                .hasValue(100L);
    }

    @Test
    void oneBrokerFailingStillReturnsTheOtherTwoAndNamesTheFailure() {
        KafkaFutureImpl<Map<String, LogDirDescription>> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new TimeoutException("no reply"));
        Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = Map.of(
                1, KafkaFuture.completedFuture(dir(10, 1)),
                3, failed,
                2, KafkaFuture.completedFuture(dir(20, 2)));

        ReplicaLogs.Read read = ReplicaLogs.read(await(), descriptions);

        assertThat(read.brokers()).containsOnlyKeys(1, 2);
        assertThat(read.current(1, ORDERS).size()).isEqualTo(10);
        assertThat(read.current(2, ORDERS).offsetLag()).isEqualTo(2);
        assertThat(read.unavailable()).contains("broker 3").contains("no reply");
        assertThat(ReplicaLogs.leaderSize(read, new Node(1, "one", 9092), ORDERS))
                .as("the leader's bytes stay when another broker fails")
                .hasValue(10L);
    }

    private static Map<String, LogDirDescription> dir(long size, long lag) {
        return Map.of("/data", new LogDirDescription(null, Map.of(ORDERS, new ReplicaInfo(size, lag, false))));
    }
}
