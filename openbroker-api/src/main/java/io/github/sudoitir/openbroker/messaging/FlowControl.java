package io.github.sudoitir.openbroker.messaging;

/**
 * How much a receiver takes in.
 *
 * @param maxUnsettled the most messages handed out and not yet settled; the receiver stops reading at it
 * @param prefetchMessages how many messages the client buffers ahead, where the system bounds by count
 * @param prefetchBytes how many bytes the client buffers ahead, where the system bounds by size
 */
public record FlowControl(int maxUnsettled, int prefetchMessages, long prefetchBytes) {

    public FlowControl {
        if (maxUnsettled < 1) {
            throw new IllegalArgumentException("maxUnsettled must be at least 1");
        }
        if (prefetchMessages < 0) {
            throw new IllegalArgumentException("prefetchMessages must not be negative");
        }
        if (prefetchBytes < 0) {
            throw new IllegalArgumentException("prefetchBytes must not be negative");
        }
    }

    /** 100 unsettled, 100 messages and 1 MiB ahead. */
    public static FlowControl defaults() {
        return new FlowControl(100, 100, 1024 * 1024);
    }

    /** One unsettled message and nothing buffered ahead, so a slow receiver leaves the rest to others. */
    public static FlowControl none() {
        return new FlowControl(1, 0, 0);
    }
}
