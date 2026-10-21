package io.github.sudoitir.openbroker.messaging;

import java.util.function.BiConsumer;

/** Watches messaging operations, for traces and metrics. Called on the thread doing the work. */
public interface MessagingObserver {

    /** A send; add entries to {@code carrier} to travel with the message as string properties. */
    Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier);

    Observation onReceive(OperationInfo op);

    Observation onProcess(OperationInfo op, Message message);

    Observation onSettle(OperationInfo op, Message message, Disposition disposition);

    /** Observes nothing, and allocates nothing. */
    static MessagingObserver none() {
        return NoObserver.INSTANCE;
    }
}
