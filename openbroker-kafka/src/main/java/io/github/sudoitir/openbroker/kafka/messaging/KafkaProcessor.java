package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import java.time.Duration;

/** A processor with the resources its lanes share, released when it stops. */
final class KafkaProcessor implements Processor {

    private final MessagingEnv env;
    private final Processor engine;
    private final AutoCloseable lanes;
    private final AutoCloseable deadLetters;

    KafkaProcessor(MessagingEnv env, Processor engine, AutoCloseable lanes, AutoCloseable deadLetters) {
        this.env = env;
        this.engine = engine;
        this.lanes = lanes;
        this.deadLetters = deadLetters;
    }

    @Override
    public String id() {
        return engine.id();
    }

    @Override
    public LinkState state() {
        return engine.state();
    }

    @Override
    public RetryPolicy retry() {
        return engine.retry();
    }

    @Override
    public void stop(Duration grace) {
        try {
            engine.stop(grace);
        } finally {
            release();
        }
    }

    @Override
    public void close() {
        try {
            engine.close();
        } finally {
            release();
        }
    }

    private void release() {
        try {
            lanes.close();
            deadLetters.close();
        } catch (Exception e) {
            // closing is best effort once the handlers have stopped
        } finally {
            env.forget(this);
        }
    }
}
