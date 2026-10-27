package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The dead-letter topics of a subscription: the policy's target, or {@code <topic>.dlt}. A record
 * that is dead-lettered is sent there with its origin topic, partition and offset, its attempts and
 * its reason as headers, and only then settled. They are created on demand; when they cannot be, the
 * open is refused, naming the topic and the remedy.
 */
final class DeadLetters implements AutoCloseable {

    private final MessagingEnv env;
    private final Topics topics;
    private final Optional<SendTarget> target;
    private final Map<String, Sender> senders = new HashMap<>();
    private final Set<String> ensured = new HashSet<>();

    DeadLetters(MessagingEnv env, Topics topics, RetryPolicy policy) {
        this.env = env;
        this.topics = topics;
        this.target = policy.deadLetter();
    }

    /** The dead-letter topic for records of {@code sourceTopic}. */
    String topicFor(String sourceTopic) {
        return target.map(Subscriptions::topicOf).orElse(sourceTopic + ".dlt");
    }

    /** Makes the dead-letter topics of {@code sourceTopics} exist, before the group is joined. */
    void ensure(Collection<String> sourceTopics) {
        for (String source : sourceTopics) {
            String dead = topicFor(source);
            int partitions =
                    topics.requirePartitions(source, Targets.topic(source).resource());
            topics.ensure(dead, partitions, "dead-letter", Targets.topic(dead).resource());
        }
    }

    /**
     * Sends {@code delivery} to its dead-letter topic.
     *
     * @param joined opens a sender joined to the caller's transaction, or null to send on its own
     */
    void reject(Delivery delivery, int attempts, String reason, @Nullable Function<SendTarget, Sender> joined) {
        ensure(List.of(delivery.originTopic()));
        String dead = topicFor(delivery.originTopic());
        OutboundMessage copy = OutboundMessage.copyOf(delivery.message())
                .withHeaders(delivery.message().headers().toBuilder()
                        .deliveryCount(null)
                        .build())
                .withProperties(Map.of(
                        RecordCodec.ATTEMPTS_PROPERTY,
                        PropertyValue.of(attempts),
                        RecordCodec.REASON_PROPERTY,
                        PropertyValue.of(reason)));
        if (joined != null) {
            joined.apply(SendTarget.destination(Targets.topic(dead).resource())).send(copy);
            return;
        }
        sender(dead).send(copy);
    }

    /** A sender to the policy's dead-letter target, for the engine to dead-letter through. */
    Sender sender(SendTarget policyTarget) {
        return sender(Subscriptions.topicOf(policyTarget));
    }

    private synchronized Sender sender(String topic) {
        return senders.computeIfAbsent(
                topic,
                t -> env.track(KafkaSender.open(
                        env, Targets.topic(t), SenderOptions.defaults().withGuarantee(Guarantee.AT_LEAST_ONCE))));
    }

    @Override
    public synchronized void close() {
        senders.values().forEach(Sender::close);
        senders.clear();
    }
}
