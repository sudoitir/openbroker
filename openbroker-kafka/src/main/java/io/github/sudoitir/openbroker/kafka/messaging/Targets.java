package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Reads where a send goes. */
final class Targets {

    private Targets() {}

    static KafkaSender.Target of(SendTarget target) {
        String topic = Subscriptions.topicOf(target);
        return switch (target) {
            case SendTarget.ToDestination d -> {
                @Nullable Integer partition = d.partition().map(p -> partition(p.value())).orElse(null);
                yield new KafkaSender.Target(topic, partition, d.routingKey(), d.destination());
            }
            case SendTarget.ToSubscription s -> new KafkaSender.Target(topic, null, Optional.empty(), s.subscription());
        };
    }

    /** The target of a send to a topic. */
    static KafkaSender.Target topic(String topic) {
        return new KafkaSender.Target(topic, null, Optional.empty(), ResourceId.of(Subscriptions.TOPIC.value(), topic));
    }

    private static int partition(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw KafkaFailures.invalid("A partition of a topic is its number; " + value + " is not one.");
        }
    }
}
