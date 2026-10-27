package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;

/**
 * The resources the messaging surface reads and writes: a topic to send to, and a consumer group
 * or share group on a topic to receive from. A group on a topic is {@code <kind>/<group>/<topic>}.
 */
public final class Subscriptions {

    /** A consumer group on a topic. */
    public static final KindId GROUP_TOPIC = KindId.of("kafka.group-topic");

    /** A share group on a topic. */
    public static final KindId SHARE_GROUP_TOPIC = KindId.of("kafka.share-group-topic");

    /** A topic. */
    public static final KindId TOPIC = KindId.of("kafka.topic");

    private Subscriptions() {}

    /**
     * A group on a topic.
     *
     * @param id the subscription's id
     * @param group the group id
     * @param topic the topic
     * @param share whether the group is a share group
     */
    public record GroupTopic(ResourceId id, String group, String topic, boolean share) {}

    /** Reads a subscription id. */
    public static GroupTopic groupTopic(ResourceId id) {
        boolean share = id.kind().equals(SHARE_GROUP_TOPIC);
        if (!share && !id.kind().equals(GROUP_TOPIC)) {
            throw invalid("A subscription of the kinds " + GROUP_TOPIC + " or " + SHARE_GROUP_TOPIC + " is read; "
                    + id.format() + " is neither.");
        }
        List<String> segments = id.path().segments();
        if (segments.size() != 2) {
            throw invalid(id.format() + " does not name a group and a topic: use " + id.kind() + "/<group>/<topic>.");
        }
        return new GroupTopic(id, segments.get(0), segments.get(1), share);
    }

    /** The topic a send target names: a topic, or the topic of a group on a topic. */
    public static String topicOf(SendTarget target) {
        ResourceId id =
                switch (target) {
                    case SendTarget.ToDestination d -> d.destination();
                    case SendTarget.ToSubscription s -> s.subscription();
                };
        if (id.kind().equals(TOPIC)) {
            if (id.path().segments().size() != 1) {
                throw invalid(id.format() + " does not name a topic: use " + TOPIC + "/<topic>.");
            }
            return id.path().segments().getFirst();
        }
        return groupTopic(id).topic();
    }

    private static BrokerException invalid(String message) {
        return new BrokerException(
                Status.INVALID_ARGUMENT, ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT), Dispatch.NOT_SENT, null, message);
    }
}
