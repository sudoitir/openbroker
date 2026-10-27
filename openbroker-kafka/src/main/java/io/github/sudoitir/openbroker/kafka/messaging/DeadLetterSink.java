package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/** Where a processor's lanes send what they give up on. */
interface DeadLetterSink {

    /**
     * Sends {@code delivery} to its dead-letter topic.
     *
     * @param joined opens a sender joined to the caller's transaction, or null to send on its own
     */
    void reject(Delivery delivery, int attempts, String reason, @Nullable Function<SendTarget, Sender> joined);

    /** A sender to the policy's dead-letter target, for the engine to dead-letter through. */
    Sender sender(SendTarget policyTarget);
}
