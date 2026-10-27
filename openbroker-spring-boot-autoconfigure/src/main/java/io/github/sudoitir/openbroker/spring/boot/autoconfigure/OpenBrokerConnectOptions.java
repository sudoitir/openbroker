package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import java.time.Clock;
import java.util.List;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Assembles the {@link ConnectOptions} of a connection from the application's beans: the call
 * interceptor, the clock, context propagation, the observers and the customizers, over the
 * {@code openbroker.client} defaults. The configured connections use it, and so can an application
 * that opens connections at runtime from its own store.
 */
public final class OpenBrokerConnectOptions {

    private final OpenBrokerProperties.Client client;
    private final ObjectProvider<CallInterceptor> interceptor;
    private final ObjectProvider<Clock> clock;
    private final ObjectProvider<ContextPropagation> contextPropagation;
    private final ObjectProvider<ObserverSource> observers;
    private final ObjectProvider<ConnectOptionsCustomizer> customizers;

    OpenBrokerConnectOptions(
            OpenBrokerProperties properties,
            ObjectProvider<CallInterceptor> interceptor,
            ObjectProvider<Clock> clock,
            ObjectProvider<ContextPropagation> contextPropagation,
            ObjectProvider<ObserverSource> observers,
            ObjectProvider<ConnectOptionsCustomizer> customizers) {
        this.client = properties.client();
        this.interceptor = interceptor;
        this.clock = clock;
        this.contextPropagation = contextPropagation;
        this.observers = observers;
        this.customizers = customizers;
    }

    /**
     * The options for a connection.
     *
     * @param name the connection's name, which tags its metrics
     * @param system the broker system, such as {@code artemis}
     * @param tls where the connection's TLS comes from; {@link TlsProvider#none()} for none
     */
    public ConnectOptions forConnection(String name, String system, TlsProvider tls) {
        return forConnection(name, system, tls, ConnectionOptions.unset());
    }

    /**
     * The listener of link events that matches the observers {@link #forConnection} wires: the Micrometer
     * recoveries counter and the OpenTelemetry observer, whichever are present, or one that does nothing.
     * A connection opened at runtime passes it to {@code Transport.watch}; configured connections do so
     * themselves.
     */
    public LinkListener linkListener(String name, String system) {
        ObserverSource source = observers.getIfAvailable();
        List<LinkListener> listeners = source == null ? List.of() : source.linkListeners(name, system);
        return event -> listeners.forEach(listener -> listener.changed(event));
    }

    /** {@link #forConnection(String, String, TlsProvider)} with the connection's own keys laid over the client defaults. */
    ConnectOptions forConnection(String name, String system, TlsProvider tls, ConnectionOptions own) {
        ConnectOptions defaults = ConnectOptions.defaults();
        ConnectOptions options = defaults.withTls(tls)
                .withTimeouts(Timeouts.fixed(
                        first(
                                own.connectTimeout(),
                                client.connectTimeout(),
                                defaults.timeouts().connect()),
                        first(
                                own.callTimeout(),
                                client.callTimeout(),
                                defaults.timeouts().call())))
                .withMaxResponseBytes(
                        first(own.maxResponseBytes(), client.maxResponseBytes(), defaults.maxResponseBytes()))
                .withSpoolDirectory(first(own.spoolDirectory(), client.spoolDirectory(), defaults.spoolDirectory()));
        CallInterceptor callInterceptor = interceptor.getIfUnique();
        if (callInterceptor != null) {
            options = options.withInterceptor(callInterceptor);
        }
        Clock theClock = clock.getIfUnique();
        if (theClock != null) {
            options = options.withClock(theClock);
        }
        ContextPropagation propagation = contextPropagation.getIfAvailable();
        if (propagation != null) {
            options = options.withContextPropagation(propagation.wrapper());
        }
        ObserverSource source = observers.getIfAvailable();
        if (source != null) {
            options = options.withObserver(source.observer(name, system));
        }
        for (ConnectOptionsCustomizer customizer : customizers.orderedStream().toList()) {
            options = customizer.customize(name, system, options);
        }
        return options;
    }

    private static <T> T first(@Nullable T own, @Nullable T client, T fallback) {
        return own != null ? own : client != null ? client : fallback;
    }

    /** What the SDK takes to carry the caller's context onto its threads. */
    record ContextPropagation(UnaryOperator<Runnable> wrapper) {}
}
