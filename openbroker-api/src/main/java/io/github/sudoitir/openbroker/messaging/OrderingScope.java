package io.github.sudoitir.openbroker.messaging;

/** Which messages are kept in order. */
public enum OrderingScope {
    /** No order. */
    NONE,
    /** Messages with one group id. */
    GROUP,
    /** Messages of one partition. */
    PARTITION,
    /** All messages of the subscription. */
    SUBSCRIPTION
}
