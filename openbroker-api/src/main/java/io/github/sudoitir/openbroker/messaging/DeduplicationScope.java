package io.github.sudoitir.openbroker.messaging;

/**
 * How far a system drops repeated messages, from the weakest to the strongest. A transport declares the
 * strongest scope it offers; a sender asks for the minimum it needs.
 *
 * <p>The scopes are not strictly nested: a {@link #WINDOW} system meets a {@link #SESSION} ask only for
 * re-sends that arrive within its window.
 *
 * <p>Typical systems: {@code SESSION} for a sequence per named producer or a producer audit; {@code
 * WINDOW} for a message-id cache such as a FIFO queue's deduplication id, a stream's message id header
 * or a duplicate-detection window; {@code NONE} for systems with no deduplication.
 */
public enum DeduplicationScope {
    /** The system drops nothing: a repeated message is stored twice. */
    NONE,
    /** The system drops the sender's own re-sends while its producer session lasts. */
    SESSION,
    /** The system drops any repeat of a caller-set deduplication id within a window. */
    WINDOW;

    /** Whether this scope is at least as strong as {@code minimum}. */
    public boolean satisfies(DeduplicationScope minimum) {
        return compareTo(minimum) >= 0;
    }
}
