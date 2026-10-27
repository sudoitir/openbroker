package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.Optional;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.SupportedVersionRange;

/**
 * What a cluster offers of share groups, read from its features. Share groups are production-ready
 * from Kafka 4.2, and a cluster says that by supporting the metadata level of 4.2 and finalizing
 * {@code share.version} at 1 or more; earlier clusters (4.1) offer a preview, which is not declared.
 * Dead-lettering by the system needs {@code share.version} 2 (Kafka 4.4).
 *
 * @param shareVersion the finalized {@code share.version}, 0 when it is off or unknown
 * @param supported the newest {@code share.version} the brokers' software supports, 0 when it has none
 * @param production whether the software is Kafka 4.2 or later
 */
public record ShareFeatures(int shareVersion, int supported, boolean production) {

    /** The first {@code metadata.version} level of Kafka 4.2 (4.2-IV0), where share groups left preview. */
    static final int PRODUCTION_METADATA_LEVEL = 28;

    /** The {@code share.version} that enables share groups. */
    public static final int GROUPS = 1;

    /** The {@code share.version} that enables dead-lettering by the system. */
    public static final int DEAD_LETTERS = 2;

    static final String FEATURE = "share.version";
    private static final String METADATA = "metadata.version";

    /** Reads the features of the cluster. */
    public static ShareFeatures read(KafkaCluster cluster) {
        FeatureMetadata metadata = cluster.call(
                "features.read",
                Optional.empty(),
                Deadlines.current(cluster.options()),
                (admin, await) -> await.get(admin.describeFeatures().featureMetadata()));
        FinalizedVersionRange finalized = metadata.finalizedFeatures().get(FEATURE);
        SupportedVersionRange supported = metadata.supportedFeatures().get(FEATURE);
        SupportedVersionRange level = metadata.supportedFeatures().get(METADATA);
        return new ShareFeatures(
                finalized == null ? 0 : finalized.maxVersionLevel(),
                supported == null ? 0 : supported.maxVersion(),
                level != null && level.maxVersion() >= PRODUCTION_METADATA_LEVEL);
    }

    /** Whether the cluster offers share groups. */
    public boolean available() {
        return production && shareVersion >= GROUPS;
    }

    /** Whether the system dead-letters share-group records. */
    public boolean deadLetters() {
        return available() && shareVersion >= DEAD_LETTERS;
    }

    /** Why share groups are not offered, empty when they are. */
    public Optional<String> unavailableReason() {
        if (available()) {
            return Optional.empty();
        }
        if (!production) {
            return Optional.of("This cluster's Kafka is older than 4.2, where share groups left preview.");
        }
        return Optional.of("Share groups are not enabled on this cluster: the feature " + FEATURE + " is finalized at "
                + shareVersion + ".");
    }

    /** What enables share groups, empty when they are offered. */
    public Optional<String> remedy() {
        if (available()) {
            return Optional.empty();
        }
        if (!production) {
            return Optional.of("Upgrade the cluster to Kafka 4.2 or later, then enable the feature with "
                    + "`kafka-features.sh upgrade --feature " + FEATURE + "=" + GROUPS + "`.");
        }
        return Optional.of("Enable the feature with `kafka-features.sh upgrade --feature " + FEATURE + "="
                + Math.max(GROUPS, Math.min(supported, DEAD_LETTERS)) + "`.");
    }
}
