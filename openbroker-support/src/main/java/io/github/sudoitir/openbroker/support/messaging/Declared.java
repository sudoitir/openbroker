package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import java.util.Set;

/** Refuses a link that asks for more than the transport declares. */
public final class Declared {

    private Declared() {}

    /**
     * Checks a link's asks against what the transport declares. Every driver calls this when it opens
     * a link, so the refusal reads the same on every system.
     *
     * @param system the system's name, as the message names it
     * @param transport what the transport declares
     * @param guarantee the guarantee asked for
     * @param deduplication the minimum deduplication scope asked for
     * @param ordering the ordering asked for
     * @param orderings the orderings this kind of link can keep: the transport's for a sender, the
     *     subscription kind's for a receiver or a processor
     * @throws BrokerException {@code UNIMPLEMENTED}, not sent, for the first ask the transport does not meet
     */
    public static void require(
            String system,
            TransportCapabilities transport,
            Guarantee guarantee,
            DeduplicationScope deduplication,
            OrderingScope ordering,
            Set<OrderingScope> orderings) {
        if (!transport.deduplication().satisfies(deduplication)) {
            throw unimplemented(system, "deduplicate in the scope " + deduplication);
        }
        if (!transport.guarantees().contains(guarantee)) {
            throw unimplemented(system, "open a link with the guarantee " + guarantee);
        }
        if (!orderings.contains(ordering)) {
            throw unimplemented(system, "keep " + ordering + " order here");
        }
    }

    private static BrokerException unimplemented(String system, String what) {
        return new BrokerException(
                Status.UNIMPLEMENTED,
                ErrorInfo.of(ErrorReasons.NOT_SUPPORTED),
                Dispatch.NOT_SENT,
                null,
                system + " cannot " + what + ".");
    }
}
