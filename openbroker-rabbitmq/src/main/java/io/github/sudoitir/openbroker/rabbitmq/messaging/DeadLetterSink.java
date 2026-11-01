package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;

/** Where a processor's lanes send what they give up on: the managed-retry dead-letter send with origin headers. */
interface DeadLetterSink {

    /** Sends {@code message} to its dead-letter target, with headers naming its origin and {@code reason}. */
    void reject(Message message, int attempts, String reason);

    /** A sender to the policy's dead-letter target, for the engine to dead-letter through. */
    Sender sender(SendTarget policyTarget);
}
