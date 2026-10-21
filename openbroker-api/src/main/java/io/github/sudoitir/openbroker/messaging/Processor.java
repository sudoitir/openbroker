package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;

/** Runs a handler on the messages of a subscription. */
public interface Processor extends AutoCloseable {

    String id();

    LinkState state();

    /** Stops taking messages, waits up to {@code grace} for running handlers, and releases the rest uncounted. */
    void stop(Duration grace);

    /** Stops with the options' {@code stopGrace}. */
    @Override
    void close();
}
