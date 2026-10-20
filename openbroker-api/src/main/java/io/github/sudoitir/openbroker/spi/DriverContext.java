package io.github.sudoitir.openbroker.spi;

import io.github.sudoitir.openbroker.ConnectOptions;
import java.util.function.Supplier;

/**
 * What a driver gets from {@code BrokerDrivers} besides the request: the caller's options, and
 * resources shared across every connection of the same {@code BrokerDrivers} (HTTP clients per TLS
 * material, for example), closed with it.
 */
public interface DriverContext {

    ConnectOptions options();

    /**
     * The shared resource under {@code key}, created by {@code factory} the first time. It is closed
     * when {@code BrokerDrivers} closes or when it is evicted.
     */
    <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory);

    /** Closes and forgets the shared resource under {@code key}, for instance after a TLS reload. */
    void evict(String key);
}
