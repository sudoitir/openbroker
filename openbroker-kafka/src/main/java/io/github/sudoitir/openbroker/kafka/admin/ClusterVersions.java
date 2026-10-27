package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.util.OptionalInt;
import org.apache.kafka.clients.admin.Admin;
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
}
