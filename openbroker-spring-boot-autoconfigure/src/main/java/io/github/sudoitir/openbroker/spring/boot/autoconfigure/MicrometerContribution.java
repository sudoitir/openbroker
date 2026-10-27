package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/** Metrics through the application's Micrometer registry: one observer for each connection. */
final class MicrometerContribution implements ObserverContribution {

    private final MeterRegistry registry;
    private final Map<String, MicrometerMessagingObserver> observers = new ConcurrentHashMap<>();

    MicrometerContribution(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public MessagingObserver observer(String connection, String system) {
        return of(connection, system);
    }

    @Override
    public @Nullable LinkListener linkListener(String connection, String system) {
        return of(connection, system);
    }

    private MicrometerMessagingObserver of(String connection, String system) {
        return observers.computeIfAbsent(
                connection + '\0' + system, key -> new MicrometerMessagingObserver(registry, connection, system));
    }
}
