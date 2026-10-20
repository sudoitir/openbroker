package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.TlsMaterial;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.remoting.impl.ssl.DefaultSSLContextFactory;
import org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextConfig;
import org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextFactory;

/**
 * Gives a Core connection the TLS material its caller supplied, so every cluster trusts only its own
 * authority and the JVM's default context is never touched.
 *
 * <p>The Artemis client finds this through {@code ServiceLoader} and prefers it over its default by
 * {@link #getPriority()}. A connection whose {@value TransportConstants#SSL_CONTEXT_PROP_NAME}
 * parameter names registered material gets that material's context; any other is handled as Artemis
 * would. The client creates the instance itself, so the material is held in one JVM-wide registry,
 * keyed by its id; a new id replaces the material it was derived from, never the other's.
 */
public final class CoreTls implements SSLContextFactory {

    /** Above Artemis's default factory (5) and its caching variant (10). */
    static final int PRIORITY = 100;

    private static final String PREFIX = "openbroker-";
    private static final Map<String, SSLContext> CONTEXTS = new ConcurrentHashMap<>();

    private final SSLContextFactory fallback = new DefaultSSLContextFactory();

    /** Registers the material and returns the connection parameter value that names it. */
    static String register(TlsMaterial material) {
        String key = PREFIX + HexFormat.of().formatHex(material.id().getBytes(StandardCharsets.UTF_8));
        CONTEXTS.put(key, material.context());
        return key;
    }

    /** Forgets material that was replaced, so its context is not kept alive. */
    static void forget(TlsMaterial material) {
        CONTEXTS.remove(PREFIX + HexFormat.of().formatHex(material.id().getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public SSLContext getSSLContext(SSLContextConfig config, Map<String, Object> params) throws Exception {
        Object name = params == null ? null : params.get(TransportConstants.SSL_CONTEXT_PROP_NAME);
        if (name == null || !name.toString().startsWith(PREFIX)) {
            return fallback.getSSLContext(config, params);
        }
        SSLContext context = CONTEXTS.get(name.toString());
        if (context == null) {
            throw new IllegalStateException("The TLS material of this Core connection was replaced; reconnect.");
        }
        return context;
    }

    @Override
    public void clearSSLContexts() {
        // The registry holds the caller's material, which only the caller replaces.
    }

    @Override
    public int getPriority() {
        return PRIORITY;
    }
}
