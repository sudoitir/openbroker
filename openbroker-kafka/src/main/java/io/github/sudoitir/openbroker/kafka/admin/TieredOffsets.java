package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.model.Attributes;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

/**
 * Local log start and the tiered offset. A topic that is not tiered reports the local start and an unknown tiered
 * offset, reason {@value #NOT_TIERED}. No number is invented for an offset the broker does not have. A broker that
 * rejects the spec is remembered on the connection and is not asked again.
 */
final class TieredOffsets {

    static final String NOT_TIERED = "not tiered";

    private TieredOffsets() {}

    /** Whether this connection may still ask for the spec. A refusal sticks. */
    static boolean supported(Context context) {
        return context.cluster()
                .decided(KafkaAccess.TIERED_OFFSETS)
                .filter(assessment -> assessment.status() == Assessment.Status.UNAVAILABLE)
                .isEmpty();
    }

    /**
     * The two offset reads, or empty maps when the spec is already refused or the broker refuses it now. Any other
     * failure is thrown.
     */
    static Read fetch(Context context, Collection<TopicPartition> partitions) {
        if (!supported(context) || partitions.isEmpty()) {
            return Read.none();
        }
        try {
            Map<TopicPartition, Long> local = Offsets.spec(context, partitions, OffsetSpec.earliestLocal());
            Map<TopicPartition, Long> tiered = Offsets.spec(context, partitions, OffsetSpec.latestTiered());
            context.cluster()
                    .decide(
                            KafkaAccess.TIERED_OFFSETS,
                            Assessment.available("The cluster serves earliest local and latest tiered offsets."));
            return new Read(local, tiered);
        } catch (BrokerException e) {
            if (!e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
                throw e;
            }
            context.cluster()
                    .decide(KafkaAccess.TIERED_OFFSETS, Assessment.unavailable(e.getMessage(), Optional.empty()));
            return Read.none();
        }
    }

    /** Writes the offsets that are known. An unknown tiered offset is the reason, not a number. */
    static void put(
            Attributes.Builder attributes,
            Map<TopicPartition, Long> local,
            Map<TopicPartition, Long> tiered,
            TopicPartition partition) {
        Long start = local.get(partition);
        if (start != null && start >= 0) {
            attributes.putRaw(KafkaKinds.LOCAL_LOG_START_OFFSET, start);
        }
        if (!tiered.containsKey(partition)) {
            return;
        }
        long offset = tiered.get(partition);
        if (offset < 0) {
            attributes.putRaw(KafkaKinds.TIERED_OFFSET_REASON, NOT_TIERED);
        } else {
            attributes.putRaw(KafkaKinds.TIERED_OFFSET, offset);
        }
    }

    record Read(Map<TopicPartition, Long> local, Map<TopicPartition, Long> tiered) {

        Read {
            local = Map.copyOf(local);
            tiered = Map.copyOf(tiered);
        }

        static Read none() {
            return new Read(Map.of(), Map.of());
        }
    }
}
