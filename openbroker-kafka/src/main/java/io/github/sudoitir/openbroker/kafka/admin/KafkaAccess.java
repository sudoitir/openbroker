package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What the account may do, without ever changing the cluster. A read is tried; a write is never
 * tried to find out, so management writes, browsing, sending and receiving are unknown until a
 * real call settles them, because Kafka authorizes each against a topic or group.
 */
public final class KafkaAccess implements Access {

    /** Moving a single message: unavailable on Kafka, whatever the account may do. */
    public static final Capability MOVE_MESSAGES = new Capability("messages.move");

    public static final Capability COPY_MESSAGES = new Capability("messages.copy");
    public static final Capability DELETE_MESSAGES = new Capability("messages.delete");

    private final KafkaCluster cluster;

    public KafkaAccess(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Assessment check(Capability capability, Optional<ResourceId> resource) {
        return probe().of(capability);
    }

    @Override
    public AccessSnapshot probe() {
        Deadline deadline = Deadlines.current(cluster.options());
        Map<Capability, Assessment> out = new LinkedHashMap<>();
        Assessment read = read(deadline);
        out.put(Capability.MANAGEMENT_READ, read);
        String settled = "Not tried: the first real call settles it.";
        if (read.status() != Assessment.Status.AVAILABLE) {
            settled = "Not assessed: management reads are not available.";
        }
        for (Capability unsettled : new Capability[] {
            Capability.MANAGEMENT_WRITE,
            Capability.NOTIFICATIONS,
            Capability.BROWSE,
            Capability.SEND,
            Capability.RECEIVE
        }) {
            out.put(unsettled, Assessment.unknown(settled));
        }
        Assessment appendOnly = Assessment.unavailable(KafkaMessages.LOG_IS_APPEND_ONLY, Optional.empty());
        out.put(MOVE_MESSAGES, appendOnly);
        out.put(COPY_MESSAGES, appendOnly);
        out.put(DELETE_MESSAGES, appendOnly);
        return new AccessSnapshot(out, cluster.options().clock().instant());
    }

    private Assessment read(Deadline deadline) {
        try {
            cluster.call(
                    "access.probe",
                    Optional.empty(),
                    deadline,
                    (admin, await) -> await.get(admin.describeCluster().clusterId()));
            return Assessment.available("The account can describe the cluster.");
        } catch (BrokerException e) {
            return switch (e.status()) {
                case PERMISSION_DENIED, UNAUTHENTICATED -> Assessment.unavailable(e.getMessage(), Optional.empty());
                default -> Assessment.unknown(e.getMessage());
            };
        }
    }
}
