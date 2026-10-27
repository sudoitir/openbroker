package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.opentelemetry.OpenTelemetryMessaging;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import org.jspecify.annotations.Nullable;

/**
 * Spans, and metrics unless Micrometer records them, through the application's {@code OpenTelemetry}. The one
 * observer serves every connection.
 */
final class OpenTelemetryContribution implements ObserverContribution {

    private final OpenTelemetryMessaging observer;

    OpenTelemetryContribution(OpenTelemetry openTelemetry, boolean metrics, boolean injectContext) {
        this.observer = OpenTelemetryMessaging.builder(metrics ? openTelemetry : new WithoutMeters(openTelemetry))
                .injectContext(injectContext)
                .build();
    }

    @Override
    public MessagingObserver observer(String connection, String system) {
        return observer;
    }

    @Override
    public @Nullable LinkListener linkListener(String connection, String system) {
        return observer;
    }

    /** The same tracer and propagators, with a meter provider that records nothing, so nothing is counted twice. */
    private record WithoutMeters(OpenTelemetry delegate) implements OpenTelemetry {

        @Override
        public TracerProvider getTracerProvider() {
            return delegate.getTracerProvider();
        }

        @Override
        public ContextPropagators getPropagators() {
            return delegate.getPropagators();
        }

        @Override
        public MeterProvider getMeterProvider() {
            return MeterProvider.noop();
        }
    }
}
