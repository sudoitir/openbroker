package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import java.time.Duration;
import java.util.List;

/** Reads messages from a subscription. Single-owner: one thread uses it at a time. */
@Incubating
public interface Receiver extends AutoCloseable {

    /** Up to {@code max} messages, waiting at most {@code timeout} for the first. */
    List<Message> receive(int max, Duration timeout);

    void settle(Message message, Disposition disposition);

    /** Settles the batch received since the last commit. A commit with no answer is {@code IN_DOUBT}. */
    void commit();

    void rollback();

    @Override
    void close();
}
