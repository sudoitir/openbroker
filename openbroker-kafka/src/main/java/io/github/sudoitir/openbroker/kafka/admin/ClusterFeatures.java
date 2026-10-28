package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.access.Remedy;
import io.github.sudoitir.openbroker.kafka.messaging.ShareFeatures;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.jspecify.annotations.Nullable;

/**
 * What a cluster is and what it offers, read once from the cluster: its mode (KRaft or ZooKeeper),
 * the release its software is, and the finalized level of each feature this driver depends on. A
 * broker does not report its release, but it reports the newest {@code metadata.version} its
 * software supports (Kafka 3.3 and later), and each release first supports a level of its own, so
 * the release is the newest one whose first level is not above it. A broker too old to report one
 * is told apart by ListTransactions (KIP-664), which Kafka 3.0 introduced.
 *
 * @param mode whether the cluster is in KRaft or ZooKeeper mode, or neither could be established
 * @param release the release, such as {@code 4.0}, or {@code at least 4.4} when it is not told apart
 * @param metadataLevel the newest {@code metadata.version} the software supports, when it says
 * @param unsupported what was found when the cluster is older than Kafka 3.0, else null
 * @param group the {@code group.version} (KIP-848)
 * @param streams the {@code streams.version}
 * @param eligibleLeaderReplicas the {@code eligible.leader.replicas.version} (KIP-966)
 * @param transaction the {@code transaction.version} (KIP-890)
 * @param share share groups
 */
public record ClusterFeatures(
        Mode mode,
        String release,
        OptionalInt metadataLevel,
        @Nullable String unsupported,
        Level group,
        Level streams,
        Level eligibleLeaderReplicas,
        Level transaction,
        ShareFeatures share) {

    static final String METADATA = "metadata.version";

    /** The first {@code metadata.version} that supports fenced broker listing (Kafka 4.0). */
    static final int FENCED_BROKERS_LEVEL = 22;

    /** The first {@code metadata.version} level of each release, from Kafka's {@code MetadataVersion} at 4.4.0. */
    private static final NavigableMap<Integer, String> RELEASES = new TreeMap<>(Map.ofEntries(
            Map.entry(7, "3.3"),
            Map.entry(8, "3.4"),
            Map.entry(9, "3.5"),
            Map.entry(12, "3.6"),
            Map.entry(15, "3.7"),
            Map.entry(20, "3.8"),
            Map.entry(21, "3.9"),
            Map.entry(22, "4.0"),
            Map.entry(26, "4.1"),
            Map.entry(28, "4.2"),
            Map.entry(30, "4.3"),
            Map.entry(31, "4.4")));

    private static final String AT_LEAST_30 = "at least 3.0";

    /** How the cluster keeps its metadata; unknown when the broker that would say did not answer. */
    public enum Mode {
        KRAFT("kraft", "KRaft mode"),
        ZOOKEEPER("zookeeper", "ZooKeeper mode"),
        UNKNOWN("unknown", "mode unknown (the controller did not answer)");

        private final String attribute;
        private final String text;

        Mode(String attribute, String text) {
            this.attribute = attribute;
            this.text = text;
        }

        /** The value of the node attribute {@code kafka.cluster_mode}. */
        public String attribute() {
            return attribute;
        }
    }

    /** One feature: the level the cluster finalized (0 when off or unknown) and the newest its software supports. */
    public record Level(int finalized, int supported) {

        boolean enabled() {
            return finalized >= 1;
        }

        static Level of(FeatureMetadata metadata, String feature) {
            FinalizedVersionRange finalized = metadata.finalizedFeatures().get(feature);
            SupportedVersionRange supported = metadata.supportedFeatures().get(feature);
            return new Level(
                    finalized == null ? 0 : finalized.maxVersionLevel(),
                    supported == null ? 0 : supported.maxVersion());
        }
    }

    /** What is known of a cluster that did not answer: nothing, and not that it is unsupported. */
    public static ClusterFeatures unread() {
        Level off = new Level(0, 0);
        return new ClusterFeatures(
                Mode.UNKNOWN, "unknown", OptionalInt.empty(), null, off, off, off, off, new ShareFeatures(0, 0, false));
    }

    /** Reads the cluster's features. */
    public static ClusterFeatures read(Admin admin, Await await) {
        FeatureMetadata metadata =
                await.get(admin.describeFeatures(new DescribeFeaturesOptions().timeoutMs(await.timeoutMs()))
                        .featureMetadata());
        SupportedVersionRange software = metadata.supportedFeatures().get(METADATA);
        OptionalInt level = software == null ? OptionalInt.empty() : OptionalInt.of(software.maxVersion());
        Mode mode = mode(admin, await, level);
        String release = null;
        String unsupported = null;
        if (level.isPresent()) {
            release = releaseOf(level.getAsInt());
            if (release == null) {
                unsupported = "the newest " + METADATA + " it supports is level " + level.getAsInt();
            }
        } else if (listsTransactions(admin, await)) {
            release = AT_LEAST_30;
        } else {
            unsupported = "it reports no " + METADATA + " and does not list transactions";
        }
        return new ClusterFeatures(
                mode,
                release == null ? "before 3.0" : release,
                level,
                unsupported,
                Level.of(metadata, "group.version"),
                Level.of(metadata, "streams.version"),
                Level.of(metadata, "eligible.leader.replicas.version"),
                Level.of(metadata, "transaction.version"),
                ShareFeatures.of(metadata));
    }

    /** The release whose software supports {@code level} as its newest, or null before the table starts. */
    static @Nullable String releaseOf(int level) {
        Map.Entry<Integer, String> found = RELEASES.floorEntry(level);
        if (found == null) {
            return null;
        }
        return found.getKey().equals(RELEASES.lastKey()) ? "at least " + found.getValue() : found.getValue();
    }

    /**
     * KRaft clusters answer a quorum description; ZooKeeper brokers do not know the request. The quorum is
     * answered by its leader, which may be the broker that cannot be reached: no answer then says nothing
     * of the mode, unless the software is Kafka 4.0 or later, which has no ZooKeeper mode.
     */
    private static Mode mode(Admin admin, Await await, OptionalInt level) {
        try {
            await.get(admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions().timeoutMs(await.timeoutMs()))
                    .quorumInfo());
            return Mode.KRAFT;
        } catch (BrokerException e) {
            Mode decided = modeAfter(e, level);
            if (decided == null) {
                throw e;
            }
            return decided;
        }
    }

    /** What a failed quorum read says of the mode; null when it says nothing the caller should absorb. */
    static @Nullable Mode modeAfter(BrokerException failure, OptionalInt level) {
        if (failure.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
            return Mode.ZOOKEEPER;
        }
        if (failure.status() == Status.PERMISSION_DENIED) {
            return Mode.KRAFT;
        }
        if (failure.status() == Status.DEADLINE_EXCEEDED || failure.status() == Status.UNAVAILABLE) {
            return level.isPresent() && level.getAsInt() >= FENCED_BROKERS_LEVEL ? Mode.KRAFT : Mode.UNKNOWN;
        }
        return null;
    }

    private static boolean listsTransactions(Admin admin, Await await) {
        try {
            await.get(admin.listTransactions(new ListTransactionsOptions().timeoutMs(await.timeoutMs()))
                    .all());
            return true;
        } catch (BrokerException e) {
            if (e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
                return false;
            }
            throw e;
        }
    }

    /** Refuses a cluster before Kafka 3.0, naming what it reported and the range this driver supports. */
    public void require(String supportedVersions) {
        if (unsupported != null) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.UNSUPPORTED_VERSION,
                    "This cluster runs a Kafka before 3.0 (" + unsupported + "); the Kafka driver supports "
                            + supportedVersions + ".");
        }
    }

    /** The cluster in a few words, such as {@code 4.0, KRaft mode}. */
    public String describe() {
        return release + ", " + mode.text;
    }

    /** Whether the cluster runs consumer-protocol groups (KIP-848). */
    public boolean consumerProtocol() {
        return group.enabled();
    }

    /** Whether the cluster runs streams groups. */
    public boolean streamsGroups() {
        return streams.enabled();
    }

    /** Whether the cluster is known to run in KRaft mode. */
    public boolean kraft() {
        return mode == Mode.KRAFT;
    }

    /** Whether the cluster lists fenced brokers, which Kafka 4.0 added to the cluster description. */
    public boolean fencedBrokers() {
        return mode == Mode.KRAFT && metadataLevel.isPresent() && metadataLevel.getAsInt() >= FENCED_BROKERS_LEVEL;
    }

    /** Whether the cluster keeps eligible leader replicas (KIP-966). */
    public boolean eligibleLeaders() {
        return eligibleLeaderReplicas.enabled();
    }

    /** The capabilities this cluster has or lacks, each with its reason and, where an operator can change it, the remedy. */
    public Map<Capability, Assessment> assessments() {
        Map<Capability, Assessment> out = new LinkedHashMap<>();
        out.put(
                KafkaAccess.QUORUM,
                switch (mode) {
                    case KRAFT ->
                        Assessment.available("This cluster runs in KRaft mode, so its controller quorum is described.");
                    case ZOOKEEPER ->
                        Assessment.unavailable(
                                "This cluster runs in ZooKeeper mode, which has no KRaft controller quorum to describe.",
                                Optional.empty());
                    case UNKNOWN ->
                        Assessment.unknown(
                                "The cluster's mode is not established: the controller did not answer, so it is not"
                                        + " known whether a KRaft controller quorum exists to describe.");
                });
        out.put(
                KafkaAccess.CONSUMER_PROTOCOL_GROUPS,
                consumerProtocol()
                        ? Assessment.available("group.version is finalized at " + group.finalized() + ".")
                        : Assessment.unavailable(
                                "Consumer-protocol groups (KIP-848) need Kafka 4.0 or later with group.version finalized"
                                        + " at 1 or more; this cluster finalizes it at " + group.finalized() + ".",
                                enable("group.version", group)));
        out.put(KafkaAccess.SHARE_GROUPS, shareAssessment());
        out.put(
                KafkaAccess.STREAMS_GROUPS,
                streamsGroups()
                        ? Assessment.available("streams.version is finalized at " + streams.finalized() + ".")
                        : Assessment.unavailable(
                                "Streams groups need streams.version finalized at 1 or more; this cluster finalizes it at "
                                        + streams.finalized() + ".",
                                enable("streams.version", streams)));
        out.put(
                KafkaAccess.FENCED_BROKERS,
                fencedBrokers()
                        ? Assessment.available("The cluster lists fenced brokers.")
                        : Assessment.unavailable(
                                "Kafka before 4.0 does not list fenced brokers, so a fenced broker is missing from the"
                                        + " nodes instead of shown as fenced.",
                                Optional.empty()));
        out.put(
                KafkaAccess.ELIGIBLE_LEADERS,
                eligibleLeaders()
                        ? Assessment.available("eligible.leader.replicas.version is finalized at "
                                + eligibleLeaderReplicas.finalized() + ".")
                        : Assessment.unavailable(
                                "Eligible leader replicas (KIP-966) are not enabled on this cluster.",
                                enable("eligible.leader.replicas.version", eligibleLeaderReplicas)));
        return out;
    }

    private Assessment shareAssessment() {
        if (share.available()) {
            return Assessment.available("share.version is finalized at " + share.shareVersion() + ".");
        }
        return Assessment.unavailable(
                share.unavailableReason().orElse(""), share.remedy().map(text -> new Remedy(text, "", "text/plain")));
    }

    /** The remedy that raises a feature, where the software supports it. */
    private static Optional<Remedy> enable(String feature, Level level) {
        if (level.supported() < 1) {
            return Optional.empty();
        }
        return Optional.of(new Remedy(
                "Enable the feature on the cluster.",
                "kafka-features.sh upgrade --feature " + feature + "=1",
                "text/plain"));
    }
}
