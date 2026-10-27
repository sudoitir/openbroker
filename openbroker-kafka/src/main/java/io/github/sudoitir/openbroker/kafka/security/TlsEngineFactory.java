package io.github.sudoitir.openbroker.kafka.security;

import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import java.io.IOException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.jspecify.annotations.Nullable;

/**
 * Makes the TLS engine of each new connection from the connection's {@link TlsProvider}, asked at
 * that moment, so reloaded material (a rotated certificate, a new trust bundle) reaches the next
 * connection with no restart. Without material the JVM's default context is used.
 *
 * <p>The server's host name is always verified against its certificate, whatever the material says:
 * Kafka's own default, the {@code https} identification, is kept.
 *
 * <p>Read from the client configuration under {@link #TLS}.
 */
public final class TlsEngineFactory implements SslEngineFactory {

    /** The client configuration key that holds the {@link TlsProvider}. */
    public static final String TLS = "openbroker.tls";

    private static final String HTTPS = "HTTPS";

    private TlsProvider provider = TlsProvider.none();

    @Override
    public void configure(Map<String, ?> configs) {
        Object supplied = configs.get(TLS);
        if (!(supplied instanceof TlsProvider tls)) {
            throw new IllegalArgumentException("The client configuration carries no " + TLS);
        }
        this.provider = tls;
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, @Nullable String endpointIdentification) {
        @Nullable TlsMaterial material = provider.current().orElse(null);
        SSLEngine engine = context(material).createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm(
                endpointIdentification == null || endpointIdentification.isBlank() ? HTTPS : endpointIdentification);
        if (material != null) {
            if (!material.protocols().isEmpty()) {
                parameters.setProtocols(material.protocols().toArray(String[]::new));
            }
            if (!material.cipherSuites().isEmpty()) {
                parameters.setCipherSuites(material.cipherSuites().toArray(String[]::new));
            }
        }
        engine.setSSLParameters(parameters);
        return engine;
    }

    private static SSLContext context(@Nullable TlsMaterial material) {
        if (material != null) {
            return material.context();
        }
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("The JVM has no default TLS context", e);
        }
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        throw new UnsupportedOperationException("A client has no server engine");
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.of();
    }

    @Override
    public @Nullable KeyStore keystore() {
        return null;
    }

    @Override
    public @Nullable KeyStore truststore() {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
