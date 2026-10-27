package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import org.jspecify.annotations.Nullable;

/** Derives a connection from what Spring Boot already knows about a broker. */
interface ConnectionDeriver {

    /** The connection's name, which is the broker system's id. */
    String name();

    /** The derived connection, or {@code null} when Spring's details do not describe one. */
    @Nullable Connection derive();
}
