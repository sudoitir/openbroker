package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLParameters;

/**
 * One {@link HttpClient} per TLS material id, shared through {@link DriverContext#shared}: no redirects, HTTP/1.1, the
 * connect timeout from the call options. A reload of the material gives it a new id, and the client for the old id is
 * evicted.
 */
final class HttpClients {

    private static final String CURRENT = "rabbitmq.http.current";

    private HttpClients() {}

    static HttpClient forTls(DriverContext context, Optional<TlsMaterial> tls, Duration connectTimeout) {
        String material = tls.map(TlsMaterial::id).orElse("plain");
        String key = "rabbitmq.http/" + material + "/" + connectTimeout.toMillis();
        String previous = current(context).put(material, key);
        if (previous != null && !previous.equals(key)) {
            context.evict(previous);
        }
        return context.shared(key, Shared.class, () -> new Shared(build(tls, connectTimeout)))
                .client();
    }

    private static Map<String, String> current(DriverContext context) {
        return context.shared(CURRENT, Current.class, () -> new Current(new ConcurrentHashMap<>()))
                .keys();
    }

    private static HttpClient build(Optional<TlsMaterial> tls, Duration connectTimeout) {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
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
            if (material.hostnameVerification()) {
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
            }
            builder.sslParameters(parameters);
        });
        return builder.build();
    }

    record Current(Map<String, String> keys) implements AutoCloseable {
        @Override
        public void close() {
            // Holds keys only; the clients are closed through their own registry entries.
        }
    }

    /** A shared client, closed when the registry is. */
    record Shared(HttpClient client) implements AutoCloseable {
        @Override
        public void close() {
            client.shutdown();
        }
    }
}
