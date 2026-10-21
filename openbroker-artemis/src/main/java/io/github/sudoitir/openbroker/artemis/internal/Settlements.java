package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;

/**
 * What settling a message with a disposition does on Artemis, for each way a receiver can hold
 * messages. The mapping is the receiver's only decision; carrying it out is its own.
 */
final class Settlements {

    /** What the receiver does. */
    enum Action {
        /** Acknowledge this message alone. */
        ACKNOWLEDGE,
        /** The broker already counted it delivered; forget it. */
        FORGET,
        /** Hand back everything the receiver holds, uncounted. */
        ROLLBACK,
        /** Hand back everything the receiver holds, counted as a failed delivery. */
        ROLLBACK_COUNTED,
        /** Send a copy to the queue to be delivered after the delay, then acknowledge. */
        COPY_AFTER_DELAY,
        /** Send a copy to the dead-letter destination, then acknowledge. */
        DEAD_LETTER,
        /** The message was handed out before the link recovered; the broker delivers it again. */
        STALE,
        /** This receiver cannot settle that way. */
        REFUSED
    }

    private Settlements() {}

    /**
     * @param guarantee how the receiver holds messages
     * @param browse whether it only reads, leaving messages where they are
     * @param stale whether the message belongs to a session the link has since replaced
     */
    static Action of(Disposition disposition, Guarantee guarantee, boolean browse, boolean stale) {
        if (browse) {
            return Action.FORGET;
        }
        if (guarantee == Guarantee.AT_MOST_ONCE) {
            return disposition instanceof Disposition.Accept ? Action.FORGET : Action.REFUSED;
        }
        if (stale) {
            return Action.STALE;
        }
        return switch (disposition) {
            case Disposition.Accept a -> Action.ACKNOWLEDGE;
            case Disposition.Release r when guarantee == Guarantee.TRANSACTED -> Action.REFUSED;
            case Disposition.Reject r when guarantee == Guarantee.TRANSACTED -> Action.REFUSED;
            case Disposition.Release r when r.delay().isPresent() -> Action.COPY_AFTER_DELAY;
            case Disposition.Release r -> r.deliveryFailed() ? Action.ROLLBACK_COUNTED : Action.ROLLBACK;
            case Disposition.Reject r -> Action.DEAD_LETTER;
        };
    }
}
