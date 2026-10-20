package io.github.sudoitir.openbroker.artemis.internal;

import java.net.URI;
import java.net.URISyntaxException;
import org.jspecify.annotations.Nullable;

/**
 * A node's {@code host:port}, the only form of its address a message or a telemetry tag carries: a
 * URL can hold {@code user:password@} and a path.
 */
public final class NodeAddress {

    public static final String UNKNOWN = "unknown";

    private NodeAddress() {}

    public static String hostPort(URI uri) {
        return hostPort(uri.toString());
    }

    /**
     * @param url a Jolokia or Core URL or a bare {@code host:port}; a Core failover list is reduced to
     *     its first node
     */
    public static String hostPort(@Nullable String url) {
        String dialable = dialable(url);
        if (dialable == null) {
            return UNKNOWN;
        }
        try {
            String bare = dialable.replaceFirst("(?i)^([a-z][a-z0-9+.-]*://)?\\(?[^@/]*@", "$1");
            URI uri = new URI(bare.replaceFirst("[,)?].*$", "").replace("(", ""));
            if (uri.getHost() == null) {
                return UNKNOWN;
            }
            return uri.getPort() < 0 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
        } catch (URISyntaxException e) {
            return UNKNOWN;
        }
    }

    /** A Core URL the client can dial: the value with {@code tcp://} added when it has no scheme. */
    public static @Nullable String dialable(@Nullable String coreUrl) {
        if (coreUrl == null || coreUrl.isBlank()) {
            return null;
        }
        String trimmed = coreUrl.trim();
        return trimmed.contains("://") ? trimmed : "tcp://" + trimmed;
    }
}
