package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * A transactional lane commits its offsets with the consumer's group metadata (KIP-447), so a lane
 * of a member that the group has dropped cannot commit: the zombie is fenced, whatever its
 * transactional id.
 */
class KafkaFencingIT {

    private static final Consuming.Sink IGNORE = new Consuming.Sink() {
        @Override
        public void deliver(Delivery delivery) {}

        @Override
        public void retract(Collection<TopicPartition> partitions) {}
    };

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
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

    private static Consumption member(MessagingEnv env, String id, ResourceId group, String topic) {
        Consumption member = Consumption.create(
                env,
                id,
                new Consumption.Settings(
                        group.path().segments().getFirst(),
                        Set.of(topic),
                        group,
                        Guarantee.TRANSACTED,
                        10,
                        0,
                        Duration.ZERO,
                        Consumption.Commits.EXTERNAL),
                IGNORE);
        member.start();
        return member;
    }

    @Test
    void aTransactionCommittingOffsetsForAMemberTheGroupDroppedIsRefused() {
        String topic = SharedKafka.topic("fence", 1);
        ResourceId group = SharedKafka.group(topic);
        MessagingEnv env = SharedKafka.env();
        try (Sender seed = SharedKafka.transport()
                .sender(SendTarget.subscription(group), Scope.whole(), SenderOptions.defaults())) {
            seed.send(KafkaMessagingIT.message(0));
        }
        Consumption old = member(env, "old", group, topic);
        await(
                "the first member has joined",
                () -> old.groupMetadata() != null && old.groupMetadata().generationId() > 0);
        ConsumerGroupMetadata stale = old.groupMetadata();
        Consumption current = member(env, "new", group, topic);
        await("the new member has the partition", () -> current.groupMetadata() != null);
        old.close();
        try (Transaction zombie = new Transaction(env.cluster(), "zombie-" + topic)) {
            zombie.ensureBegun();
            assertThatThrownBy(
                            () -> zombie.commit(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(1)), stale))
                    .isInstanceOf(KafkaException.class);
            zombie.abort();
        } finally {
            old.close();
            current.close();
            env.close();
        }
        assertThat(stale.generationId()).isPositive();
    }
}
