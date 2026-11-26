package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.schema.SchemaCatalog;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.DescribeClusterOptions;

/**
 * What the account may do, without ever changing the cluster. A read is tried; a write is never
 * tried to find out, so management writes, browsing, sending and receiving are unknown until a
 * real call settles them, because Kafka authorizes each against a topic or group.
 */
public final class KafkaAccess implements Access {

    /** The cluster runs in KRaft mode, so its controller quorum can be described. */
    public static final Capability QUORUM = new Capability("kafka.quorum");

    public static final Capability CONSUMER_PROTOCOL_GROUPS = new Capability("kafka.consumer-protocol-groups");
    public static final Capability SHARE_GROUPS = new Capability("kafka.share-groups");
    public static final Capability STREAMS_GROUPS = new Capability("kafka.streams-groups");
    public static final Capability FENCED_BROKERS = new Capability("kafka.fenced-brokers");
    public static final Capability ELIGIBLE_LEADERS = new Capability("kafka.eligible-leaders");

    /** The cluster lists transactions and the producers in them (KIP-664), which Kafka 3.0 added. */
    public static final Capability TRANSACTION_LISTING = new Capability("kafka.transaction-listing");

    /**
     * Earliest-local and latest-tiered offsets, which Kafka 3.9 added. Probed once per connection; a broker that
     * rejects the spec is unavailable, with its reason, and is not asked again.
     */
    public static final Capability TIERED_OFFSETS = new Capability("kafka.tiered-offsets");

    /** Preferred leader election. Unavailable, with the broker's reason, when the broker rejects the call. */
    public static final Capability PREFERRED_ELECTION = new Capability("kafka.preferred-election");

    /** Partition reassignment. Unavailable, with the broker's reason, when the broker rejects the call. */
    public static final Capability REASSIGNMENT = new Capability("kafka.reassignment");

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
        for (Capability unsettled :
                new Capability[] {Capability.MANAGEMENT_WRITE, Capability.BROWSE, Capability.SEND, Capability.RECEIVE
                }) {
            out.put(unsettled, Assessment.unknown(settled));
        }
        out.put(
                Capability.NOTIFICATIONS,
                read.status() == Assessment.Status.AVAILABLE
                        ? Assessment.available("Kafka sends no events; the watch lists the cluster every "
                                + cluster.watchInterval() + " with the read permission this account holds.")
                        : Assessment.unknown(settled));
        Assessment appendOnly = Assessment.unavailable(KafkaMessages.LOG_IS_APPEND_ONLY, Optional.empty());
        out.put(Capability.MOVE_MESSAGES, appendOnly);
        out.put(Capability.COPY_MESSAGES, appendOnly);
        out.put(Capability.DELETE_MESSAGES, appendOnly);
        if (read.status() == Assessment.Status.AVAILABLE) {
            try {
                out.putAll(cluster.features().assessments());
            } catch (BrokerException _) {
                // A cluster that would not say what it offers leaves those capabilities out.
            }
            try {
                out.put(TIERED_OFFSETS, cluster.tieredOffsets());
            } catch (BrokerException e) {
                out.put(TIERED_OFFSETS, Assessment.unknown(e.getMessage()));
            }
            out.put(PREFERRED_ELECTION, reported(cluster, PREFERRED_ELECTION, "Preferred leader election is sent."));
            out.put(REASSIGNMENT, reported(cluster, REASSIGNMENT, "Partition reassignment is sent."));
        }
        out.put(SchemaCatalog.SCHEMA_REGISTRY, registry());
        return new AccessSnapshot(out, cluster.options().clock().instant());
    }

    /**
     * A registry is independent of what the account may do on the cluster. Named is not reached: this probe does not
     * call it, so a named registry stays unknown until a catalog call does.
     */
    private Assessment registry() {
        String kind = cluster.option(KafkaOptions.REGISTRY_KIND, KafkaOptions.REGISTRY_NONE);
        if (kind.isBlank() || KafkaOptions.REGISTRY_NONE.equals(kind)) {
            return Assessment.unavailable("No schema registry is configured on this connection", Optional.empty());
        }
        if (!KafkaOptions.REGISTRY_CONFLUENT.equals(kind)) {
            return Assessment.unavailable(
                    "This driver does not speak a registry kind of " + kind + ".", Optional.empty());
        }
        return Assessment.unknown("A schema registry is named on this connection but has not been reached.");
    }

    /** The refusal recorded on this connection, or available until a broker rejects the call. */
    private static Assessment reported(KafkaCluster cluster, Capability capability, String available) {
        return cluster.decided(capability).orElseGet(() -> Assessment.available(available));
    }

    private Assessment read(Deadline deadline) {
        try {
            cluster.call(
                    "access.probe",
                    Optional.empty(),
                    deadline,
                    (admin, await) ->
                            await.get(admin.describeCluster(new DescribeClusterOptions().timeoutMs(await.timeoutMs()))
                                    .clusterId()));
            return Assessment.available("The account can describe the cluster.");
        } catch (BrokerException e) {
            return switch (e.status()) {
                case PERMISSION_DENIED, UNAUTHENTICATED -> Assessment.unavailable(e.getMessage(), Optional.empty());
                default -> Assessment.unknown(e.getMessage());
            };
        }
    }
}
