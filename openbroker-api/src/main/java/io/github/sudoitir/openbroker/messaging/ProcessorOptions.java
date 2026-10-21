package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Objects;

/**
 * How a processor runs its handler.
 *
 * @param receive how each lane reads; must consume
 * @param concurrency the number of lanes, 1 to 64
 * @param ordering which messages the handler sees in order; {@code SUBSCRIPTION} needs one lane
 * @param retry what happens when the handler fails
 * @param stopGrace how long {@code close()} waits for running handlers
 */
public record ProcessorOptions(
        ReceiverOptions receive, int concurrency, OrderingScope ordering, RetryPolicy retry, Duration stopGrace) {

    /** The most lanes. */
    public static final int MAX_CONCURRENCY = 64;

    public ProcessorOptions {
        Objects.requireNonNull(receive, "receive");
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(stopGrace, "stopGrace");
        if (receive.mode() != ReceiveMode.CONSUME) {
            throw new IllegalArgumentException("a processor consumes");
        }
        if (concurrency < 1 || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException("concurrency must be between 1 and " + MAX_CONCURRENCY);
        }
        if (ordering == OrderingScope.SUBSCRIPTION && concurrency != 1) {
            throw new IllegalArgumentException("subscription ordering needs a concurrency of 1");
        }
        if (stopGrace.isNegative()) {
            throw new IllegalArgumentException("stopGrace must not be negative");
        }
    }

    /** Consuming, 8 lanes, unordered, the system's retry policy, a 30 s grace. */
    public static ProcessorOptions defaults() {
        return new ProcessorOptions(
                ReceiverOptions.consume(), 8, OrderingScope.NONE, RetryPolicy.system(), Duration.ofSeconds(30));
    }

    public ProcessorOptions withConcurrency(int concurrency) {
        return new ProcessorOptions(receive, concurrency, ordering, retry, stopGrace);
    }

    public ProcessorOptions withOrdering(OrderingScope ordering) {
        return new ProcessorOptions(receive, concurrency, ordering, retry, stopGrace);
    }

    public ProcessorOptions withRetry(RetryPolicy retry) {
        return new ProcessorOptions(receive, concurrency, ordering, retry, stopGrace);
    }
}
