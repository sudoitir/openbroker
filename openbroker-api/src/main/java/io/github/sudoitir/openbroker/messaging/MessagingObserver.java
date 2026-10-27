package io.github.sudoitir.openbroker.messaging;

import java.util.Arrays;
import java.util.List;
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

    /**
     * These observers as one, in order, for a connection that feeds metrics and traces to different
     * places. {@link #none()} entries are dropped: nothing left is {@link #none()}, one left is itself.
     * An observer that throws is skipped for that operation and never stops another or the operation.
     */
    static MessagingObserver composite(MessagingObserver... observers) {
        List<MessagingObserver> kept =
                Arrays.stream(observers).filter(o -> o != NoObserver.INSTANCE).toList();
        return switch (kept.size()) {
            case 0 -> none();
            case 1 -> kept.getFirst();
            default -> new CompositeObserver(kept);
        };
    }
}
