package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class TransportsTest {

    @Test
    void connectionsWithTheSameMaterialAndTimeoutShareOneClient() {
        Context context = new Context(timeouts(2));

        assertThat(Transports.http(context, Optional.empty())).isSameAs(Transports.http(context, Optional.empty()));
    }

    @Test
    void aNewConnectTimeoutReleasesTheClientBuiltForTheOldOne() {
        Context context = new Context(timeouts(2));
        HttpClient old = Transports.http(context, Optional.empty());

        context.options = timeouts(3);
        HttpClient fresh = Transports.http(context, Optional.empty());

        assertThat(fresh).isNotSameAs(old);
        assertThat(old.isTerminated() || isShuttingDown(old)).isTrue();
    }

    @Test
    void twoRegistriesNeverReleaseEachOthersClients() {
        Context first = new Context(timeouts(2));
        Context second = new Context(timeouts(3));
        HttpClient kept = Transports.http(first, Optional.empty());

        Transports.http(second, Optional.empty());

        assertThat(Transports.http(first, Optional.empty())).isSameAs(kept);
    }

    @Test
    void aClientNeverFollowsARedirect() {
        assertThat(Transports.http(new Context(timeouts(2)), Optional.empty()).followRedirects())
                .isEqualTo(HttpClient.Redirect.NEVER);
    }

    private static ConnectOptions timeouts(int connectSeconds) {
        return ConnectOptions.defaults()
                .withTimeouts(Timeouts.fixed(Duration.ofSeconds(connectSeconds), Duration.ofSeconds(10)));
    }

    /** A shut-down client refuses new requests; the JDK reports that only once in-flight ones finish. */
    private static boolean isShuttingDown(HttpClient client) {
        try {
            return client.awaitTermination(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** One registry of shared resources, as {@code BrokerDrivers} keeps it. */
    private static final class Context implements DriverContext {

        private final Map<String, AutoCloseable> shared = new ConcurrentHashMap<>();
        ConnectOptions options;

        Context(ConnectOptions options) {
            this.options = options;
        }

        @Override
        public ConnectOptions options() {
            return options;
        }

        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return type.cast(shared.computeIfAbsent(key, k -> factory.get()));
        }

        @Override
        public void evict(String key) {
            AutoCloseable old = shared.remove(key);
            if (old != null) {
                try {
                    old.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
