package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLParameters;

/**
 * The HTTP clients the driver shares across connections: one per TLS material and timeout, never one
 * per call or per connection, because each owns a selector thread and a socket pool until it is
 * closed. A reload of the material gives it a new id, and the client for the old id is evicted.
 */
public final class Transports {

    private static final String CURRENT = "artemis.http.current";

    private Transports() {}

    public static HttpClient http(DriverContext context, Optional<TlsMaterial> tls) {
        ConnectOptions options = context.options();
        String material = tls.map(TlsMaterial::id).orElse("plain");
        String key =
                "artemis.http/" + material + "/" + options.timeouts().connect().toMillis();
        String previous = current(context).put(material, key);
        if (previous != null && !previous.equals(key)) {
            context.evict(previous);
        }
        return context.shared(key, Shared.class, () -> new Shared(build(options, tls)))
                .client();
    }

    /** Forgets the client built from this material, so the next call builds one from fresh material. */
    public static void evict(DriverContext context, String materialId) {
        String key = current(context).remove(materialId);
        if (key != null) {
            context.evict(key);
        }
    }

    /** The key last used per TLS material in this context, so a client built for an old connect timeout is released. */
    private static Map<String, String> current(DriverContext context) {
        return context.shared(CURRENT, Current.class, () -> new Current(new ConcurrentHashMap<>()))
                .keys();
    }

    private static HttpClient build(ConnectOptions options, Optional<TlsMaterial> tls) {
        // A Jolokia agent never redirects; the Artemis console does, to its login page, and following
        // that turns a wrong path into an unreadable answer.
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(options.timeouts().connect())
                .followRedirects(HttpClient.Redirect.NEVER);
        tls.ifPresent(material -> {
            builder.sslContext(material.context());
            SSLParameters parameters = material.context().getDefaultSSLParameters();
            if (!material.protocols().isEmpty()) {
                parameters.setProtocols(material.protocols().toArray(String[]::new));
            }
            if (!material.cipherSuites().isEmpty()) {
                parameters.setCipherSuites(material.cipherSuites().toArray(String[]::new));
            }
            builder.sslParameters(parameters);
        });
        return builder.build();
    }

    record Current(Map<String, String> keys) implements AutoCloseable {
        @Override
        public void close() {}
    }

    /** A shared client, closed when the registry is. */
    record Shared(HttpClient client) implements AutoCloseable {
        @Override
        public void close() {
            client.shutdown();
        }
    }
}
