package io.github.sudoitir.openbroker.messaging;

import java.util.function.BiConsumer;
import org.jspecify.annotations.Nullable;

/** The shared observer and observation that do nothing. */
enum NoObserver implements MessagingObserver, Observation {
    INSTANCE;

    @Override
    public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
        return this;
    }

    @Override
    public Observation onReceive(OperationInfo op) {
        return this;
    }

    @Override
    public Observation onProcess(OperationInfo op, Message message) {
        return this;
    }

    @Override
    public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
        return this;
    }

    @Override
    public void end(@Nullable Throwable error) {}
}
