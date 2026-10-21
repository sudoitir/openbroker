package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Optional;

/** Sends messages to one target. Thread-safe unless {@code TRANSACTED}, which has a single owner. */
public interface Sender extends AutoCloseable {

    String id();

    LinkState state();

    /** The deduplication window of the target; empty when the system does not deduplicate. */
    Optional<DeduplicationWindow> deduplication();

    /** Waits for room for up to {@code maxBlock}, then for the guarantee's confirmation. */
    SendReceipt send(OutboundMessage message);

    /** Takes the message without waiting; {@link Offer.Full} when the buffer has no room. */
    Offer offer(OutboundMessage message);

    /** Waits until every accepted message is confirmed, or fails with {@code DEADLINE_EXCEEDED}. */
    void flush(Duration timeout);

    /** Commits the batch sent since the last commit. A commit with no answer is {@code IN_DOUBT}. */
    void commit();

    void rollback();

    /** Flushes for {@code maxBlock}, then fails the rest as {@code CANCELLED}. */
    @Override
    void close();
}
