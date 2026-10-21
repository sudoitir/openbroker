package io.github.sudoitir.openbroker.messaging;

/** The unit in which a system bounds what it delivers ahead of the reader. */
public enum PrefetchBound {
    /** A count of messages. */
    MESSAGES,
    /** A number of bytes. */
    BYTES
}
