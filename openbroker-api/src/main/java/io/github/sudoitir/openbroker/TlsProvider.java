package io.github.sudoitir.openbroker;

import java.util.Optional;

/**
 * Supplies the TLS material for a connection. A driver asks on every connect and reconnect, and
 * releases transports built from material whose id the provider no longer returns.
 *
 * <p>Empty means no TLS is configured for the cluster: an endpoint whose scheme requires TLS (such as
 * {@code https}) uses the JVM's default trust, and a protocol where TLS is optional connects without
 * it.
 */
public interface TlsProvider {

    Optional<TlsMaterial> current();

    /**
     * Registers a callback for when the material changes, so a driver can release what it built from
     * the old material. The default never calls it.
     */
    default void onChange(Runnable listener) {}

    static TlsProvider none() {
        return Optional::empty;
    }

    static TlsProvider of(TlsMaterial material) {
        Optional<TlsMaterial> fixed = Optional.of(material);
        return () -> fixed;
    }
}
