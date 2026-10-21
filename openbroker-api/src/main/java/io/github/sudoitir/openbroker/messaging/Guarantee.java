package io.github.sudoitir.openbroker.messaging;

/**
 * What a link promises about the messages it moves. Fixed when the link opens. "Effectively once" is
 * {@link #AT_LEAST_ONCE} with an idempotent sender.
 */
public enum Guarantee {
    /** Sent or received without waiting for confirmation: a message may be lost, never duplicated by the SDK. */
    AT_MOST_ONCE,
    /** Confirmed before it counts as sent or settled; after a failure a message may be repeated. */
    AT_LEAST_ONCE,
    /** Sends and settlements take effect together on {@code commit()}, or not at all. */
    TRANSACTED
}
