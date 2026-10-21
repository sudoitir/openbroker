package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** The answer to {@link Sender#offer}: taken, or no room. */
public sealed interface Offer {

    /** The sender took the message; the stage completes with its receipt, or fails with a {@code BrokerException}. */
    record Accepted(CompletionStage<SendReceipt> receipt) implements Offer {
        public Accepted {
            Objects.requireNonNull(receipt, "receipt");
        }
    }

    /** The buffer is full: {@code inFlight} messages and {@code bufferedBytes} bytes are unconfirmed. */
    record Full(int inFlight, long bufferedBytes) implements Offer {}
}
