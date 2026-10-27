package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.ConnectOptions;

/**
 * Adjusts the {@link ConnectOptions} of every connection the starter assembles, configured or opened
 * at runtime, the way {@code RestClientCustomizer} adjusts a REST client. Customizers run last, in
 * {@code @Order} order, after the beans, the observers and the {@code openbroker.client} defaults are
 * in place.
 */
@FunctionalInterface
public interface ConnectOptionsCustomizer {

    /**
     * Returns the options to connect with.
     *
     * @param connection the connection's name
     * @param system the broker system, such as {@code kafka}
     * @param options the options assembled so far
     */
    ConnectOptions customize(String connection, String system, ConnectOptions options);
}
