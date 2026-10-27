package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.TlsProvider;
import org.jspecify.annotations.Nullable;

/** Derives a connection from what Spring Boot already knows about a broker. */
interface ConnectionDeriver {

    /**
     * What Spring's details say of a connection.
     *
     * @param connection the connection they describe
     * @param tls where its TLS comes from, when Spring supplies an SSL bundle
     * @param credentialsNeeded why the connection needs credentials of its own, when Spring's details
     *     use a form of authentication that cannot be derived; {@code null} when it needs none
     */
    record Derived(
            Connection connection,
            @Nullable TlsProvider tls,
            @Nullable String credentialsNeeded) {}

    /** The connection's name, which is the broker system's id. */
    String name();

    /** The derived connection, or {@code null} when Spring's details do not describe one. */
    @Nullable Derived derive();
}
