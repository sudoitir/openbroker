package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.util.OptionalInt;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.SupportedVersionRange;

/**
 * Tells whether a cluster runs Kafka 4.0 or later. A broker does not report its release, but it
 * reports the newest {@code metadata.version} its software supports, and 4.0 is the first release
 * that supports level {@value #FIRST_SUPPORTED_LEVEL} (4.0-IV0; 3.9 stops at 21). That is read
 * from the software, not from the finalized level, so a 4.x cluster that has not yet raised its
 * metadata version is still accepted.
 */
final class ClusterVersions {

    static final String FEATURE = "metadata.version";
    static final int FIRST_SUPPORTED_LEVEL = 22;

    private ClusterVersions() {}

    /** Refuses a cluster before Kafka 4.0, naming what it reported and the range this driver supports. */
    static void require(Admin admin, Await await, String supportedVersions) {
        OptionalInt highest = highestSupportedLevel(admin, await);
        if (!isSupported(highest)) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.UNSUPPORTED_VERSION,
                    "This cluster runs a Kafka before 4.0 ("
                            + describe(highest)
                            + "); the Kafka driver supports "
                            + supportedVersions
                            + ".");
        }
    }

    /** The newest metadata version the answering broker's software supports; none if it does not say. */
    private static OptionalInt highestSupportedLevel(Admin admin, Await await) {
        try {
            SupportedVersionRange range = await.get(admin.describeFeatures().featureMetadata())
                    .supportedFeatures()
                    .get(FEATURE);
            return range == null ? OptionalInt.empty() : OptionalInt.of(range.maxVersion());
        } catch (BrokerException e) {
            // A broker too old to know the request is older than 4.0 too.
            if (e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
                return OptionalInt.empty();
            }
            throw e;
        }
    }

    static boolean isSupported(OptionalInt highestSupportedLevel) {
        return highestSupportedLevel.isPresent() && highestSupportedLevel.getAsInt() >= FIRST_SUPPORTED_LEVEL;
    }

    static String describe(OptionalInt highestSupportedLevel) {
        return highestSupportedLevel.isEmpty()
                ? "it reports no " + FEATURE
                : "the newest " + FEATURE + " it supports is level " + highestSupportedLevel.getAsInt();
    }

    static final String SHARE_FEATURE = "share.version";
    /** The metadata version of 4.2-IV0, the first release in which share groups are production-ready; 4.1 had a preview. */
    static final int FIRST_SHARE_PRODUCTION_LEVEL = 28;

    /**
     * Whether the cluster runs share groups as a production feature: its software is 4.2 or later
     * (4.1 shipped them as a preview behind the same {@value #SHARE_FEATURE}) and the feature is
     * finalized at level 1 or more.
     */
    static boolean sharesInProduction(Admin admin, Await await) {
        FeatureMetadata features = await.get(admin.describeFeatures().featureMetadata());
        SupportedVersionRange software = features.supportedFeatures().get(FEATURE);
        FinalizedVersionRange share = features.finalizedFeatures().get(SHARE_FEATURE);
        return software != null
                && software.maxVersion() >= FIRST_SHARE_PRODUCTION_LEVEL
                && share != null
                && share.maxVersionLevel() >= 1;
    }
}
