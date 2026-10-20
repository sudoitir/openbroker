package io.github.sudoitir.openbroker;

import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/**
 * The TLS a connection uses. The {@code id} names this material: connections with the same id share
 * their transports, and new material (a reloaded bundle) gets a new id.
 *
 * @param hostnameVerification whether the server's certificate must match its host name; a driver
 *     whose transport cannot turn it off verifies regardless and says so
 * @param protocols the TLS protocol versions allowed; empty means the context's defaults
 * @param cipherSuites the cipher suites allowed; empty means the context's defaults
 */
public record TlsMaterial(
        String id,
        SSLContext context,
        boolean hostnameVerification,
        List<String> protocols,
        List<String> cipherSuites) {

    public TlsMaterial {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(context, "context");
        protocols = List.copyOf(protocols);
        cipherSuites = List.copyOf(cipherSuites);
    }

    public static TlsMaterial of(String id, SSLContext context) {
        return new TlsMaterial(id, context, true, List.of(), List.of());
    }
}
