package io.github.sudoitir.openbroker;

/**
 * Supplies the TLS material for a connection. A driver asks on every connect and reconnect, and
 * releases transports built from material whose id the provider no longer returns.
 */
public interface TlsProvider {

    TlsMaterial current();

    /**
     * Registers a callback for when the material changes, so a driver can release what it built from
     * the old material. The default never calls it.
     */
    default void onChange(Runnable listener) {}

    static TlsProvider systemDefault() {
        TlsMaterial material = TlsMaterial.systemDefault();
        return () -> material;
    }

    static TlsProvider of(TlsMaterial material) {
        return () -> material;
    }
}
