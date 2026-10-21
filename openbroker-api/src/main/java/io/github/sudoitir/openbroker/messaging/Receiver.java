package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.List;

/** Reads messages from a subscription. Single-owner: one thread uses it at a time. */
public interface Receiver extends AutoCloseable {

    String id();

    LinkState state();

    /** Up to {@code max} messages, waiting at most {@code timeout} for the first. Stops at {@code maxUnsettled}; empty while {@code RECOVERING}. */
    List<Message> receive(int max, Duration timeout);

    void settle(Message message, Disposition disposition);

    /** A sender joined to this {@code TRANSACTED} receiver's transaction. */
    Sender sender(SendTarget target);

    /** Settles the batch received since the last commit. A commit with no answer is {@code IN_DOUBT}. */
    void commit();

    void rollback();

    @Override
    void close();
}
