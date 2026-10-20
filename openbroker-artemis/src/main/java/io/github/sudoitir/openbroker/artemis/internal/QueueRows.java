package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * The rows of {@code listQueues}, as queue resources. Internal queues are broker plumbing and are
 * skipped.
 */
public final class QueueRows {

    private QueueRows() {}

    public static List<Resource> parse(@Nullable JsonNode data, NodeId node, Instant observedAt) {
        List<Resource> out = new ArrayList<>();
        if (data == null || !data.isArray()) {
            return out;
        }
        for (JsonNode row : data) {
            if (ArtemisLists.flag(row, "internalQueue")) {
                continue;
            }
            String name = ArtemisLists.str(row, "name");
            String address = ArtemisLists.str(row, "address");
            if (name == null || name.isEmpty()) {
                continue;
            }
            String routingType = ArtemisLists.str(row, "routingType");
            Attributes.Builder a = Attributes.builder()
                    .put(StandardAttributes.DURABLE, ArtemisLists.flag(row, "durable"))
                    .put(StandardAttributes.PAUSED, ArtemisLists.flag(row, "paused"))
                    .putRaw(ArtemisKinds.ROUTING_TYPE, routingType == null ? "ANYCAST" : routingType.toUpperCase())
                    .put(MetricKeys.BACKLOG, MetricValue.exact(ArtemisLists.num(row, "messageCount")))
                    .put(MetricKeys.CONSUMER_COUNT, MetricValue.exact(ArtemisLists.num(row, "consumerCount")))
                    .put(MetricKeys.IN_FLIGHT, MetricValue.exact(ArtemisLists.num(row, "deliveringCount")))
                    .put(MetricKeys.SCHEDULED, MetricValue.exact(ArtemisLists.num(row, "scheduledCount")))
                    .put(MetricKeys.ENQUEUED, MetricValue.exact(ArtemisLists.num(row, "messagesAdded")))
                    .put(MetricKeys.ACKNOWLEDGED, MetricValue.exact(ArtemisLists.num(row, "messagesAcked")))
                    .put(MetricKeys.EXPIRED, MetricValue.exact(ArtemisLists.num(row, "messagesExpired")));
            if (address != null && !address.isEmpty()) {
                a.put(StandardAttributes.DESTINATION, new ResourceId(ArtemisKinds.ADDRESS, ResourcePath.of(address)));
            }
            out.add(new Resource(
                    new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of(name)),
                    a.build(),
                    List.of(),
                    List.of(Placement.instance(node, Attributes.empty())),
                    observedAt));
        }
        return out;
    }
}
