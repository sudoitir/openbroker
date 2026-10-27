package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.ssl.NoSuchSslBundleException;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.ssl.SslOptions;

/**
 * TLS for a connection from a Spring SSL bundle ({@code spring.ssl.bundle}). A provider reads the bundle
 * again on every connect, so a bundle that reloads ({@code reload-on-update}) reaches the next
 * connection, and it tells the SDK of the reload through {@link TlsProvider#onChange}.
 */
public final class SslBundleTls {

    private final SslBundles bundles;
    private final Map<String, ReloadingProvider> named = new ConcurrentHashMap<>();

    SslBundleTls(SslBundles bundles) {
        this.bundles = bundles;
    }

    /**
     * TLS from the bundle called {@code bundleName}, current on every connect.
     *
     * @param bundleName the name under {@code spring.ssl.bundle}
     * @param verifyHostname whether the server's host name is checked against its certificate
     * @throws BrokerException {@code UNAVAILABLE} with the reason {@code TLS_HANDSHAKE_FAILED}, not sent,
     *     when no such bundle exists
     */
    public TlsProvider forBundle(String bundleName, boolean verifyHostname) {
        return named.computeIfAbsent(bundleName + '\0' + verifyHostname, key -> {
            ReloadingProvider provider = new ReloadingProvider(bundleName, verifyHostname);
            provider.watch();
            return provider;
        });
    }

    /** TLS from {@code bundle} as it is now; a bundle object does not reload. */
    public TlsProvider forBundle(SslBundle bundle, boolean verifyHostname) {
        Optional<TlsMaterial> material =
                Optional.of(material("ssl-bundle-" + UUID.randomUUID(), bundle, verifyHostname));
        return () -> material;
    }

    private static TlsMaterial material(String id, SslBundle bundle, boolean verifyHostname) {
        SslOptions options = bundle.getOptions();
        return new TlsMaterial(
                id,
                bundle.createSslContext(),
                verifyHostname,
                list(options.getEnabledProtocols()),
                list(options.getCiphers()));
    }

    private static List<String> list(String @Nullable [] values) {
        return values == null ? List.of() : List.of(values);
    }

    private static BrokerException missing(String bundleName, NoSuchSslBundleException cause) {
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.TLS_HANDSHAKE_FAILED),
                Dispatch.NOT_SENT,
                null,
                "The SSL bundle '" + bundleName + "' does not exist; add it under spring.ssl.bundle",
                cause);
    }

    /**
     * A bundle looked up by name on every connect, whose reload is passed on. Each bundle object it meets
     * gets a new material id, which is how the SDK tells a rotation.
     */
    private final class ReloadingProvider implements TlsProvider {

        private final String bundleName;
        private final boolean verifyHostname;
        private final AtomicLong serial = new AtomicLong();
        private final List<Runnable> listeners = new CopyOnWriteArrayList<>();
        private volatile @Nullable Material cached;

        ReloadingProvider(String bundleName, boolean verifyHostname) {
            this.bundleName = bundleName;
            this.verifyHostname = verifyHostname;
        }

        void watch() {
            try {
                bundles.addBundleUpdateHandler(bundleName, updated -> listeners.forEach(Runnable::run));
            } catch (NoSuchSslBundleException e) {
                throw missing(bundleName, e);
            }
        }

        @Override
        public Optional<TlsMaterial> current() {
            SslBundle bundle;
            try {
                bundle = bundles.getBundle(bundleName);
            } catch (NoSuchSslBundleException e) {
                throw missing(bundleName, e);
            }
            Material last = cached;
            if (last == null || last.bundle != bundle) {
                last = new Material(
                        bundle, material(bundleName + "@" + serial.incrementAndGet(), bundle, verifyHostname));
                cached = last;
            }
            return Optional.of(last.material);
        }

        @Override
        public void onChange(Runnable listener) {
            listeners.add(listener);
        }
    }

    private record Material(SslBundle bundle, TlsMaterial material) {}
}
