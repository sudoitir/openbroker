package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

/** The topic questions the messaging surface asks the Admin client: how many partitions, and a topic to be there. */
final class Topics {

    private static final int LOOKUPS = 5;
    private static final long LOOKUP_PAUSE = 200;

    private final KafkaCluster cluster;

    Topics(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    /** The partitions of {@code topic}, or empty when there is no such topic. */
    OptionalInt partitions(String topic, ResourceId target) {
        try {
            TopicDescription description = cluster.call(
                    "topic.describe",
                    Optional.of(target),
                    Deadlines.current(cluster.options()),
                    (admin, await) -> await.get(admin.describeTopics(
                                            List.of(topic), new DescribeTopicsOptions().timeoutMs(await.timeoutMs()))
                                    .allTopicNames())
                            .get(topic));
            return OptionalInt.of(description.partitions().size());
        } catch (BrokerException e) {
            if (ErrorReasons.RESOURCE_GONE.equals(e.reason())) {
                return OptionalInt.empty();
            }
            throw e;
        }
    }

    /**
     * The partitions of {@code topic}, failing {@code NOT_FOUND} when there is no such topic. A topic
     * created a moment ago may not have reached the broker that answers yet, so a missing one is
     * asked for a few times before it is called missing.
     */
    int requirePartitions(String topic, ResourceId target) {
        for (int attempt = 1; ; attempt++) {
            OptionalInt partitions = partitions(topic, target);
            if (partitions.isPresent()) {
                return partitions.getAsInt();
            }
            if (attempt == LOOKUPS) {
                throw KafkaFailures.refused(
                        Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, "The topic " + topic + " does not exist.");
            }
            try {
                Thread.sleep(LOOKUP_PAUSE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Deadlines.cancelled(KafkaCluster.CLUSTER, e);
            }
        }
    }

    /**
     * Makes {@code topic} exist, creating it with {@code partitions} partitions and the cluster's
     * default replication when it is missing.
     *
     * @param purpose what the topic is for, as the refusal names it ("dead-letter", "retry")
     * @throws BrokerException {@code FAILED_PRECONDITION}, naming the topic and the remedy, when it is
     *     missing and cannot be created
     */
    void ensure(String topic, int partitions, String purpose, ResourceId target) {
        if (partitions(topic, target).isPresent()) {
            return;
        }
        try {
            cluster.call("topic.create", Optional.of(target), Deadlines.current(cluster.options()), (admin, await) -> {
                await.get(admin.createTopics(List.of(new NewTopic(topic, Optional.of(partitions), Optional.empty())))
                        .all());
                return null;
            });
        } catch (BrokerException e) {
            if (ErrorReasons.RESOURCE_EXISTS.equals(e.reason())) {
                return;
            }
            throw KafkaFailures.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.PRECONDITION_FAILED,
                    "The " + purpose + " topic " + topic + " does not exist and could not be created ("
                            + e.getMessage() + "). Create it with " + partitions + " partitions, for example "
                            + "`kafka-topics.sh --create --topic " + topic + " --partitions " + partitions
                            + "`, give the account permission to create topics, or name an existing topic as the target.");
        }
    }
}
