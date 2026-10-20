package io.github.sudoitir.openbroker;

import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The entry point: the drivers available, and connections through them. One instance owns the
 * resources its drivers share across connections (such as HTTP clients per TLS material) and
 * releases them on {@link #close()}; keep one per application, and close it after its connections.
 *
 * <pre>{@code
 * try (BrokerDrivers drivers = BrokerDrivers.load();
 *         ClusterConnection cluster = drivers.connect(new ConnectRequest("artemis", spec, credentials, options))) {
 *     cluster.resources().read(ResourceQuery.of(ResourceClass.SUBSCRIPTION));
 * }
 * }</pre>
 */
public final class BrokerDrivers implements AutoCloseable {

    private final Map<String, BrokerDriver> drivers;
    private final Map<String, AutoCloseable> shared = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private BrokerDrivers(List<BrokerDriver> drivers) {
        Map<String, BrokerDriver> byId = new LinkedHashMap<>();
        for (BrokerDriver d : drivers) {
            if (byId.putIfAbsent(d.system().id(), d) != null) {
                throw new IllegalStateException(
                        "Two drivers for system " + d.system().id());
            }
        }
        this.drivers = Map.copyOf(byId);
    }

    /**
     * The drivers on the SDK's own class or module path. The thread's context class loader is never
     * consulted, so code loaded by a child class loader cannot add a driver and receive credentials.
     */
    public static BrokerDrivers load() {
        List<BrokerDriver> found = new ArrayList<>();
        ServiceLoader.load(BrokerDriver.class, BrokerDriver.class.getClassLoader())
                .forEach(found::add);
        return new BrokerDrivers(found);
    }

    /** Exactly these drivers, for a container that wires them itself. */
    public static BrokerDrivers of(List<BrokerDriver> drivers) {
        return new BrokerDrivers(List.copyOf(drivers));
    }

    public List<BrokerSystem> systems() {
        return drivers.values().stream().map(BrokerDriver::system).toList();
    }

    public SystemDescriptor descriptor(String system) {
        BrokerDriver d = driver(system);
        return new SystemDescriptor(d.system(), d.schema());
    }

    /** Reaches the cluster once and reports what it found; nothing stays open. */
    public ConnectionReport verify(ConnectRequest request) {
        return driver(request.system()).verify(request.cluster(), request.credentials(), context(request.options()));
    }

    public ClusterConnection connect(ConnectRequest request) {
        return driver(request.system()).connect(request.cluster(), request.credentials(), context(request.options()));
    }

    /**
     * Releases the resources the drivers share. Close the connections made through this instance
     * first; their owners close them.
     */
    @Override
    public void close() {
        closed = true;
        shared.values().forEach(BrokerDrivers::closeQuietly);
        shared.clear();
    }

    private BrokerDriver driver(String system) {
        if (closed) {
            throw new IllegalStateException("BrokerDrivers is closed");
        }
        BrokerDriver d = drivers.get(Objects.requireNonNull(system, "system"));
        if (d == null) {
            throw new IllegalArgumentException("No driver for system " + system + "; available: " + drivers.keySet());
        }
        return d;
    }

    private DriverContext context(ConnectOptions options) {
        return new DriverContext() {
            @Override
            public ConnectOptions options() {
                return options;
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return type.cast(shared.computeIfAbsent(key, k -> factory.get()));
            }

            @Override
            public void evict(String key) {
                AutoCloseable old = shared.remove(key);
                if (old != null) {
                    closeQuietly(old);
                }
            }
        };
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            System.getLogger(BrokerDrivers.class.getName())
                    .log(System.Logger.Level.WARNING, "Closing " + c.getClass().getSimpleName() + " failed", e);
        }
    }
}
