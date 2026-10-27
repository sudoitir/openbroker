package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

class ReplicationTest {

    private static final Node ONE = new Node(1, "one", 9092);
    private static final Node TWO = new Node(2, "two", 9092);
    private static final Node THREE = new Node(3, "three", 9092);

    @Test
    void aReplicaOutsideTheInSyncSetIsCountedOnItsBroker() {
        TopicDescription topic = new TopicDescription(
                "orders",
                false,
                List.of(
                        new TopicPartitionInfo(0, ONE, List.of(ONE, TWO, THREE), List.of(ONE, TWO)),
                        new TopicPartitionInfo(1, TWO, List.of(ONE, TWO, THREE), List.of(TWO, THREE)),
                        new TopicPartitionInfo(2, ONE, List.of(ONE, TWO, THREE), List.of(ONE, TWO))));

        assertThat(Replication.outOfSyncPartitions(List.of(topic))).isEqualTo(Map.of(3, 2L, 1, 1L));
    }

    @Test
    void fullyReplicatedPartitionsCountNothing() {
        TopicDescription topic = new TopicDescription(
                "orders", false, List.of(new TopicPartitionInfo(0, ONE, List.of(ONE, TWO), List.of(TWO, ONE))));

        assertThat(Replication.outOfSyncPartitions(List.of(topic))).isEmpty();
    }
}
