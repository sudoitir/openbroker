package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import java.util.HashMap;
import java.util.Map;

/** Where a processor's lanes send what they give up on: the managed-retry dead-letter send with origin headers. */
interface DeadLetterSink {

    /** The property that holds how many times the message was tried. */
    String ATTEMPTS_PROPERTY = "openbroker.dead_letter_attempts";

    /** The property that holds why the message was given up on. */
    String REASON_PROPERTY = "openbroker.dead_letter_reason";

    /** Sends {@code message} to its dead-letter target, with headers naming its origin and {@code reason}. */
    void reject(Message message, int attempts, String reason);

    /** A sender to the policy's dead-letter target, for the engine to dead-letter through. */
    Sender sender(SendTarget policyTarget);

    /** The sink over one sender per target, each confirming its sends. */
    final class Senders implements DeadLetterSink, AutoCloseable {

        private final LinkEnv env;
        private final RetryPolicy policy;
        private final Map<SendTarget, Sender> senders = new HashMap<>();

        Senders(LinkEnv env, RetryPolicy policy) {
            this.env = env;
            this.policy = policy;
        }

        @Override
        public void reject(Message message, int attempts, String reason) {
            SendTarget target = policy.deadLetter()
                    .orElseThrow(() -> LinkEnv.precondition("The retry policy names no dead-letter target."));
            OutboundMessage copy = OutboundMessage.copyOf(message)
                    .withHeaders(message.headers().toBuilder()
                            .deliveryCount(null)
                            .origin(message.ref().source().format())
                            .originId(message.headers().id().orElse(null))
                            .build())
                    .withProperties(Map.of(
                            ATTEMPTS_PROPERTY, PropertyValue.of(attempts), REASON_PROPERTY, PropertyValue.of(reason)));
            sender(target).send(copy);
        }

        @Override
        public synchronized Sender sender(SendTarget policyTarget) {
            return senders.computeIfAbsent(
                    policyTarget, target -> env.track(new RabbitMqSender(env, target, SenderOptions.defaults())));
        }

        @Override
        public synchronized void close() {
            senders.values().forEach(Sender::close);
            senders.clear();
        }
    }
}
