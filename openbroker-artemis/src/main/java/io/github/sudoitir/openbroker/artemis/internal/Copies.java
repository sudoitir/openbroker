package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * The copies a receiver sends in place of a message it settles: one that is delivered again after a
 * delay, one for the dead-letter destination. Both are new messages: they get a new id, are not
 * mistaken for a duplicate of the original by the broker's dedupe cache, and keep everything else.
 */
final class Copies {

    /** The attempt a message is on, kept by the copy a delayed release sends. */
    static final String ATTEMPT = "openbroker.attempt";

    /** Why a message was dead-lettered. */
    static final String DEAD_LETTER_REASON = "openbroker.dead_letter_reason";

    private Copies() {}

    /**
     * The attempt a message is on: the broker's delivery count, or the attempt a delayed copy carries,
     * whichever is more. A copy is a new message to the broker, so its own count starts again.
     */
    static int attempt(Message message, int delivered) {
        int attempt = delivered;
        PropertyValue carried = message.properties().get(ATTEMPT);
        if (carried != null && carried.boxed() instanceof Number n) {
            attempt = Math.max(attempt, n.intValue());
        }
        return attempt;
    }

    /** The message on the attempt it is, as the handler sees it. */
    static Message withAttempt(Message message, int delivered) {
        return new Message(
                message.ref(),
                message.headers().toBuilder()
                        .deliveryCount(attempt(message, delivered))
                        .build(),
                message.properties(),
                message.body(),
                message.truncated(),
                message.extensions());
    }

    /**
     * A copy to deliver after {@code delay}, on the next attempt when the release counts as a failed
     * delivery and on the same one when it does not.
     */
    static OutboundMessage delayed(Message message, Duration delay, boolean counted, Instant now) {
        int attempt = message.headers().deliveryCount().orElse(1);
        return OutboundMessage.copyOf(message)
                .withHeaders(message.headers().toBuilder()
                        .id(null)
                        .deduplicationId(null)
                        .deliveryCount(null)
                        .deliverAt(now.plus(delay))
                        .build())
                .withProperties(Map.of(ATTEMPT, PropertyValue.of(counted ? attempt + 1 : attempt)));
    }

    /** A copy for the dead-letter destination, carrying why. */
    static OutboundMessage deadLetter(Message message, String reason) {
        return OutboundMessage.copyOf(message)
                .withHeaders(message.headers().toBuilder()
                        .id(null)
                        .deduplicationId(null)
                        .deliveryCount(null)
                        .deliverAt(null)
                        .build())
                .withProperties(Map.of(DEAD_LETTER_REASON, PropertyValue.of(reason)));
    }
}
