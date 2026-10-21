package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.UnaryOperator;
import java.util.random.RandomGenerator;

/**
 * What the engine classes share with the connection: the clock, the random source for jitter and
 * identifiers, how to wait, the wrapper for tasks handed to another thread, the observer and the
 * interceptor. A driver builds one per connection with {@link #of(ConnectOptions)}; a test supplies
 * its own clock, random source and sleeper.
 *
 * @param clock the time source
 * @param random the jitter and identifier source; it must be thread-safe
 * @param sleeper how a backoff is waited out
 * @param contextPropagation wraps every task started on a virtual thread
 * @param observer watches sends and handlers
 * @param interceptor admits the recovery calls
 */
public record EngineContext(
        Clock clock,
        RandomGenerator random,
        Sleeper sleeper,
        UnaryOperator<Runnable> contextPropagation,
        MessagingObserver observer,
        CallInterceptor interceptor) {

    private static final RandomGenerator SYSTEM_RANDOM =
            () -> ThreadLocalRandom.current().nextLong();

    public EngineContext {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(random, "random");
        Objects.requireNonNull(sleeper, "sleeper");
        Objects.requireNonNull(contextPropagation, "contextPropagation");
        Objects.requireNonNull(observer, "observer");
        Objects.requireNonNull(interceptor, "interceptor");
    }

    /** The context of one connection: its clock, propagation, observer and interceptor. */
    public static EngineContext of(ConnectOptions options) {
        return new EngineContext(
                options.clock(),
                SYSTEM_RANDOM,
                Sleeper.system(),
                options.contextPropagation(),
                options.observer(),
                options.interceptor());
    }

    /** Starts {@code task} on a named virtual thread, wrapped by the context propagation. */
    public Thread start(String name, Runnable task) {
        return Thread.ofVirtual().name(name).start(contextPropagation.apply(task));
    }

    /** Runs each task on its own virtual thread: where receipts and listeners complete. */
    public Executor completions() {
        return task -> start("openbroker-completion", task);
    }
}
