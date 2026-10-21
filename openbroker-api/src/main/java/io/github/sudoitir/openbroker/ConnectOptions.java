package io.github.sudoitir.openbroker;

import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * How a connection behaves, independent of the system: the caller's controls and the limits that
 * keep a connection safe.
 *
 * @param interceptor admits and observes every management and control call
 * @param tls the TLS material, asked on every connect
 * @param clock the time source for deadlines and timestamps
 * @param contextPropagation wraps every task a driver hands to another thread, so the caller's own
 *     context (a principal, a trace) reaches its interceptor there
 * @param spoolDirectory where large message bodies are streamed to instead of held in memory
 * @param maxResponseBytes the largest management response a driver reads; larger ones fail
 * @param timeouts how long connecting, and a call made outside a {@link CallScope}, may take
 * @param observer watches messaging operations
 */
public record ConnectOptions(
        CallInterceptor interceptor,
        TlsProvider tls,
        Clock clock,
        UnaryOperator<Runnable> contextPropagation,
        Path spoolDirectory,
        long maxResponseBytes,
        Timeouts timeouts,
        MessagingObserver observer) {

    public ConnectOptions {
        Objects.requireNonNull(interceptor, "interceptor");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(contextPropagation, "contextPropagation");
        Objects.requireNonNull(spoolDirectory, "spoolDirectory");
        Objects.requireNonNull(timeouts, "timeouts");
        Objects.requireNonNull(observer, "observer");
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
    }

    /**
     * No interception, no TLS beyond the JVM's defaults, the UTC system clock, a spool directory under
     * the temporary directory, 256 MiB responses, a 3 s connect timeout, a 10 s call timeout and no observer.
     */
    public static ConnectOptions defaults() {
        return new ConnectOptions(
                CallInterceptor.none(),
                TlsProvider.none(),
                Clock.systemUTC(),
                UnaryOperator.identity(),
                Path.of(System.getProperty("java.io.tmpdir"), "openbroker-spool"),
                256L * 1024 * 1024,
                Timeouts.fixed(Duration.ofSeconds(3), Duration.ofSeconds(10)),
                MessagingObserver.none());
    }

    public ConnectOptions withInterceptor(CallInterceptor interceptor) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withTls(TlsProvider tls) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withClock(Clock clock) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withContextPropagation(UnaryOperator<Runnable> contextPropagation) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withSpoolDirectory(Path spoolDirectory) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withMaxResponseBytes(long maxResponseBytes) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withTimeouts(Timeouts timeouts) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }

    public ConnectOptions withObserver(MessagingObserver observer) {
        return new ConnectOptions(
                interceptor, tls, clock, contextPropagation, spoolDirectory, maxResponseBytes, timeouts, observer);
    }
}
