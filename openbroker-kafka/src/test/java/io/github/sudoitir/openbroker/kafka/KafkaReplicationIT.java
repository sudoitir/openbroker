package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.admin.KafkaAccess;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Partition placement on the default image: tiered offsets, and later a replica move. */
class KafkaReplicationIT {

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
    }

    @AfterAll
    static void stop() {
        connection.close();
        drivers.close();
    }

    @Test
    void aTopicThatIsNotTieredReportsLocalStartAndAnUnknownTieredOffset() {
        String name = Kafka.unique("tier");
        ResourceId topic = new ResourceId(KafkaKinds.TOPIC, ResourcePath.of(name));
        connection
                .operations()
                .execute(
                        topic,
                        new Operation.Create(Attributes.builder()
                                .putRaw(KafkaKinds.PARTITIONS, 1L)
                                .putRaw(KafkaKinds.REPLICATION_FACTOR, 3L)
                                .build()),
                        Scope.whole())
                .orThrow();
        try {
            Resource partition = Kafka.eventually(
                    () -> partition(name).orElse(null),
                    resource -> resource != null
                            && resource.attributes()
                                    .raw(KafkaKinds.LOCAL_LOG_START_OFFSET)
                                    .isPresent());

            assertThat(partition.attributes().raw(KafkaKinds.LOCAL_LOG_START_OFFSET))
                    .isEqualTo(partition.attributes().raw(KafkaKinds.LOG_START_OFFSET));
            assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET)).isEmpty();
            assertThat(partition.attributes().raw(KafkaKinds.TIERED_OFFSET_REASON))
                    .contains("not tiered");
            assertThat(connection
                            .access()
                            .probe()
                            .of(KafkaAccess.TIERED_OFFSETS)
                            .status())
                    .isEqualTo(Assessment.Status.AVAILABLE);
        } finally {
            connection
                    .operations()
                    .execute(topic, Operation.delete(), Scope.whole())
                    .orThrow();
        }
    }

    private static Optional<Resource> partition(String topic) {
        return connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.PARTITION).where(Map.of("topic", topic)))
                .orThrow()
                .items()
                .stream()
                .findFirst();
    }
}
