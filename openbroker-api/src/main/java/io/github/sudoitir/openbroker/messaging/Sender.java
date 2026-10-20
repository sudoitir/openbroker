package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;

/** Sends messages to one target. Single-owner: one thread uses it at a time. */
@Incubating
public interface Sender extends AutoCloseable {

    SendReceipt send(OutboundMessage message);

    /** Commits the batch sent since the last commit. A commit with no answer is {@code IN_DOUBT}. */
    void commit();

    void rollback();

    @Override
    void close();
}
