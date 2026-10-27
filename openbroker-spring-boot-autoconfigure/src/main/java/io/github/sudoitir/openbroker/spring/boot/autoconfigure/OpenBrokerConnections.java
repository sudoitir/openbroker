package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.messaging.Transport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

/**
 * The connections of the application: those under {@code openbroker.connections} and those derived
 * from Spring Boot's connection details. It binds and checks each one, opens its
 * {@link ClusterConnection} and gets its {@link Transport}; the registrar defines a bean for each.
 */
public final class OpenBrokerConnections implements DisposableBean {

    static final String BEAN_NAME = "openBrokerConnections";

    private final Environment environment;
    private final BrokerDrivers drivers;
    private final OpenBrokerConnectOptions connectOptions;
    private final ObjectProvider<SslBundleTls> ssl;
    private final ObjectProvider<ConnectionDeriver> derivers;
    private final ListableBeanFactory beans;
    private final Map<String, Connection> bound = new ConcurrentHashMap<>();
    private final Map<String, Optional<ConnectionDeriver.Derived>> derivedByName = new ConcurrentHashMap<>();
    private final List<AutoCloseable> watches = new ArrayList<>();

    OpenBrokerConnections(
            Environment environment,
            BrokerDrivers drivers,
            OpenBrokerConnectOptions connectOptions,
            ObjectProvider<SslBundleTls> ssl,
            ObjectProvider<ConnectionDeriver> derivers,
            ListableBeanFactory beans) {
        this.environment = environment;
        this.drivers = drivers;
        this.connectOptions = connectOptions;
        this.ssl = ssl;
        this.derivers = derivers;
        this.beans = beans;
    }

    /** The names under {@code openbroker.connections}. */
    static Set<String> configuredNames(Environment environment) {
        Map<String, Object> declared = Binder.get(environment)
                .bind(
                        ConnectionBinder.PREFIX.substring(0, ConnectionBinder.PREFIX.length() - 1),
                        Bindable.mapOf(String.class, Object.class))
                .orElse(Map.of());
        return new TreeSet<>(declared.keySet());
    }

    /** The names of the connections: the configured ones and those derived from Spring's details. */
    public Set<String> names() {
        Set<String> names = configuredNames(environment);
        derivers.orderedStream().forEach(deriver -> {
            if (derived(deriver.name()) != null) {
                names.add(deriver.name());
            }
        });
        return names;
    }

    /** The connection called {@code name}, bound, checked against its driver and with defaults filled in. */
    public Connection connection(String name) {
        return bound.computeIfAbsent(name, key -> {
            ConnectionDeriver.Derived derived = derived(key);
            return new ConnectionBinder(environment, drivers).bind(key, derived == null ? null : derived.connection());
        });
    }

    private ConnectionDeriver.@Nullable Derived derived(String name) {
        return derivedByName
                .computeIfAbsent(
                        name,
                        key -> derivers.orderedStream()
                                .filter(deriver -> deriver.name().equals(key))
                                .findFirst()
                                .map(ConnectionDeriver::derive))
                .orElse(null);
    }

    /**
     * Opens the connection called {@code name}; the bean of that name calls this once. A connection that
     * was to be derived but is not (an embedded broker) gives no bean.
     */
    @Nullable ClusterConnection open(String name) {
        if (derived(name) == null && !configuredNames(environment).contains(name)) {
            return null;
        }
        Connection connection = connection(name);
        String system = connection.system();
        String bundle = connection.options().sslBundle();
        ConnectionDeriver.Derived derived = derived(name);
        TlsProvider tls = derived != null && derived.tls() != null ? derived.tls() : TlsProvider.none();
        if (bundle != null) {
            SslBundleTls bundleTls = ssl.getIfAvailable();
            if (bundleTls == null) {
                throw new ConnectionConfigurationException(
                        name,
                        ConnectionBinder.PREFIX + name + ".options.ssl-bundle",
                        "needs Spring Boot's SSL bundle support, which is not on the class path");
            }
            tls = bundleTls.forBundle(bundle, connection.options().hostnameVerified());
        }
        ConnectOptions options = connectOptions.forConnection(name, system, tls, connection.options());
        return drivers.connect(
                new ConnectRequest(system, connection.cluster(), credentials(name, connection, derived), options));
    }

    /** The messaging transport of an open connection, or {@code null} when its driver offers none. */
    @Nullable Transport transport(String name, @Nullable ClusterConnection opened) {
        Optional<Transport> transport = opened == null ? Optional.empty() : opened.transport();
        if (transport.isEmpty()) {
            return null;
        }
        synchronized (watches) {
            watches.add(transport
                    .get()
                    .watch(connectOptions.linkListener(name, connection(name).system())));
        }
        return transport.get();
    }

    /** A {@code CredentialSource} bean qualified with the connection's name wins over its properties. */
    private CredentialSource credentials(
            String name, Connection connection, ConnectionDeriver.@Nullable Derived derived) {
        for (String beanName : BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beans, CredentialSource.class)) {
            if (BeanFactoryAnnotationUtils.isQualifierMatch(name::equals, beanName, beans)) {
                return beans.getBean(beanName, CredentialSource.class);
            }
        }
        String prefix = ConnectionBinder.PREFIX + name;
        if (derived != null
                && derived.credentialsNeeded() != null
                && !connection.credentials().isSet()) {
            throw new ConnectionConfigurationException(
                    name,
                    prefix + ".credentials",
                    derived.credentialsNeeded() + "; set the connection's credentials under " + prefix
                            + ".credentials or define a CredentialSource bean qualified '" + name + "'");
        }
        Credentials data = connection.credentials().isSet()
                ? connection.credentials().toCredentials(name, prefix + ".credentials")
                : new Credentials.None();
        Credentials management = connection.managementCredentials().isSet()
                ? connection.managementCredentials().toCredentials(name, prefix + ".management-credentials")
                : data;
        return plane -> plane == Plane.MANAGEMENT ? management : data;
    }

    @Override
    public void destroy() {
        synchronized (watches) {
            for (AutoCloseable watch : watches) {
                try {
                    watch.close();
                } catch (Exception _) {
                    // The transport is closing with its connection; a watch that will not close is gone with it.
                }
            }
            watches.clear();
        }
    }
}
