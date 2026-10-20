package io.github.sudoitir.openbroker;

import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/**
 * The TLS a connection uses: an {@link SSLContext} and whether host names are verified. The {@code
 * id} names this material; connections with the same id share their transports, and a new id means
 * new material (a reloaded bundle gets a new id).
 */
public record TlsMaterial(String id, SSLContext context, boolean hostnameVerification) {

    public TlsMaterial {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(context, "context");
    }

    /** The JVM's default trust and key material, with host names verified. */
    public static TlsMaterial systemDefault() {
        try {
            return new TlsMaterial("system-default", SSLContext.getDefault(), true);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("The JVM has no default TLS context", e);
        }
    }
}
