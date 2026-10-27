package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A cluster that has share groups but whose {@code share.version} is 1, so the broker does not
 * dead-letter: the managed policy dead-letters by sending the record to a topic and then rejecting it.
 * The newest image is downgraded for the class and raised again after.
 */
class KafkaShareFallbackIT {

    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    private static void setShareVersion(int level, FeatureUpdate.UpgradeType type) {
        try (Admin admin =
                Admin.create(Map.of("bootstrap.servers", SharedKafka.cluster().bootstrapServers()))) {
            admin.updateFeatures(
                            Map.of("share.version", new FeatureUpdate((short) level, type)),
                            new UpdateFeaturesOptions())
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not set share.version to " + level, e);
        }
    }

    @BeforeAll
    static void downgrade() {
        SharedKafka.assumeDeadLetteringShareGroups();
        setShareVersion(1, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE);
        drivers = BrokerDrivers.load();
        connection = drivers.connect(SharedKafka.request(ConnectOptions.defaults()));
    }

    @AfterAll
    static void restore() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
            drivers.close();
        } finally {
            setShareVersion(2, FeatureUpdate.UpgradeType.UPGRADE);
        }
    }

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

    @Test
    void shareGroupsAreDeclaredWithoutSystemDeadLettering() {
        Transport transport = connection.transport().orElseThrow();
        assertThat(transport
                        .capabilities()
                        .subscription(Subscriptions.SHARE_GROUP_TOPIC)
                        .retry())
                .isEqualTo(new SystemRetry(false, true, false));
    }

    @Test
    void aManagedPolicyDeadLettersBySendingToATopicAndThenRejecting() {
        Transport transport = connection.transport().orElseThrow();
        String topic = SharedKafka.topic("share-fallback", 1);
        ResourceId group = SharedKafka.shareGroup(topic, Map.of());
        try (Sender sender =
                transport.sender(SendTarget.subscription(group), Scope.whole(), SenderOptions.defaults())) {
            sender.send(KafkaMessagingIT.message(3));
        }
        AtomicInteger calls = new AtomicInteger();
        ProcessorOptions managed = new ProcessorOptions(
                ReceiverOptions.consume(),
                1,
                OrderingScope.NONE,
                RetryPolicy.managed(2, new Backoff(Duration.ofMillis(50), 1.0, Duration.ofMillis(50), 0)),
                Duration.ofSeconds(30));
        try (Processor processor = transport.process(group, Scope.whole(), managed, ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("never");
        })) {
            await("the dead letter topic to hold the record", () -> deadLetterCount(topic + ".dlt") >= 1);
        }
        assertThat(calls.get()).isEqualTo(2);
        assertThat(deadLetterCount(topic + ".dlt")).isEqualTo(1);
    }

    private static long deadLetterCount(String topic) {
        try (Admin admin =
                Admin.create(Map.of("bootstrap.servers", SharedKafka.cluster().bootstrapServers()))) {
            var partition = new TopicPartition(topic, 0);
            return admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .all()
                    .get()
                    .get(partition)
                    .offset();
        } catch (Exception e) {
            return 0;
        }
    }
}
