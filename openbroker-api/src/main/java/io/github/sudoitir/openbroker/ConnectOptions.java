package io.github.sudoitir.openbroker;

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
 * @param connectTimeout how long establishing a connection may take
 * @param callTimeout the deadline of a call made outside a {@link CallScope}
 */
public record ConnectOptions(
        CallInterceptor interceptor,
        TlsProvider tls,
        Clock clock,
        UnaryOperator<Runnable> contextPropagation,
        Path spoolDirectory,
        long maxResponseBytes,
        Duration connectTimeout,
        Duration callTimeout) {

    public ConnectOptions {
        Objects.requireNonNull(interceptor, "interceptor");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(contextPropagation, "contextPropagation");
        Objects.requireNonNull(spoolDirectory, "spoolDirectory");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(callTimeout, "callTimeout");
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
    }

    /**
     * No interception, the JVM's TLS defaults, the UTC system clock, a spool directory under the
     * temporary directory, 256 MiB responses, a 3 s connect timeout and a 10 s call timeout.
     */
    public static ConnectOptions defaults() {
        return new ConnectOptions(
                CallInterceptor.none(),
                TlsProvider.systemDefault(),
                Clock.systemUTC(),
                UnaryOperator.identity(),
                Path.of(System.getProperty("java.io.tmpdir"), "openbroker-spool"),
                256L * 1024 * 1024,
                Duration.ofSeconds(3),
                Duration.ofSeconds(10));
    }

    public ConnectOptions withInterceptor(CallInterceptor interceptor) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withTls(TlsProvider tls) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withClock(Clock clock) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withContextPropagation(UnaryOperator<Runnable> contextPropagation) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withSpoolDirectory(Path spoolDirectory) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withMaxResponseBytes(long maxResponseBytes) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }

    public ConnectOptions withTimeouts(Duration connectTimeout, Duration callTimeout) {
        return new ConnectOptions(
                interceptor,
                tls,
                clock,
                contextPropagation,
                spoolDirectory,
                maxResponseBytes,
                connectTimeout,
                callTimeout);
    }
}
