package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

/** The settings of a share group that a processor on it needs to know, and the one it sets. */
record ShareGroupConfigs(String deadLetterTopic, int deliveryLimit) {

    static final String DEAD_LETTER_TOPIC = "errors.deadletterqueue.topic.name";
    static final String DELIVERY_LIMIT = "share.delivery.count.limit";
    static final String AUTO_OFFSET_RESET = "share.auto.offset.reset";

    private static final int DEFAULT_DELIVERY_LIMIT = 5;

    /** Whether the group names a topic for the broker to dead-letter to. */
    boolean namesDeadLetterTopic() {
        return !deadLetterTopic.isBlank();
    }

    /** Reads the group's effective configuration, defaults included. */
    static ShareGroupConfigs read(KafkaCluster cluster, String group, ResourceId target) {
        Config config = cluster.call(
                "group.configs", Optional.of(target), Deadlines.current(cluster.options()), (admin, await) -> {
                    ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, group);
                    return await.get(admin.describeConfigs(List.of(resource)).all())
                            .get(resource);
                });
        Map<String, ConfigEntry> entries = new HashMap<>();
        config.entries().forEach(entry -> entries.put(entry.name(), entry));
        String topic = Optional.ofNullable(entries.get(DEAD_LETTER_TOPIC))
                .map(ConfigEntry::value)
                .orElse("");
        int limit = Optional.ofNullable(entries.get(DELIVERY_LIMIT))
                .map(ConfigEntry::value)
                .map(Integer::parseInt)
                .orElse(DEFAULT_DELIVERY_LIMIT);
        return new ShareGroupConfigs(topic, limit);
    }

    /**
     * Makes a new group read from the start of its topic, as a consumer group of this driver does, unless
     * its offset reset was set on purpose. Best effort: an account that may not alter the group's
     * configuration leaves it as it is.
     */
    static void readFromTheStart(KafkaCluster cluster, String group, ResourceId target) {
        try {
            cluster.call("group.configs", Optional.of(target), Deadlines.current(cluster.options()), (admin, await) -> {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, group);
                Config config = await.get(
                                admin.describeConfigs(List.of(resource)).all())
                        .get(resource);
                ConfigEntry current = config.get(AUTO_OFFSET_RESET);
                if (current != null && current.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG) {
                    return null;
                }
                Map<ConfigResource, Collection<AlterConfigOp>> change = Map.of(
                        resource,
                        List.of(new AlterConfigOp(
                                new ConfigEntry(AUTO_OFFSET_RESET, "earliest"), AlterConfigOp.OpType.SET)));
                await.get(admin.incrementalAlterConfigs(change).all());
                return null;
            });
        } catch (RuntimeException e) {
            System.getLogger(ShareGroupConfigs.class.getName())
                    .log(System.Logger.Level.DEBUG, "The group's offset reset was left as it is", e);
        }
    }
}
