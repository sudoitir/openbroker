package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/** A driver that declares options and opens connections that do nothing but record what happens. */
class FakeDriver implements BrokerDriver {

    static final String SYSTEM = "fake";

    static final ConnectionSchema SCHEMA = new ConnectionSchema(List.of(
            new OptionSpec(
                    "mode",
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.empty(),
                    List.of("a", "b"),
                    "mode",
                    Optional.empty()),
            new OptionSpec(
                    "pin",
                    OptionSpec.Type.INTEGER,
                    false,
                    true,
                    Optional.empty(),
                    List.of(),
                    "a secret number",
                    Optional.empty())));

    /** What a connect call was given. */
    record Request(ClusterSpec cluster, CredentialSource credentials, ConnectOptions options) {}

    final List<Request> requests = new CopyOnWriteArrayList<>();
    final List<String> events = new CopyOnWriteArrayList<>();
    final List<LinkListener> watchers = new CopyOnWriteArrayList<>();
    BooleanSupplier driversOpen = () -> true;
    boolean messaging = true;

    @Override
    public BrokerSystem system() {
        return new BrokerSystem(SYSTEM, "Fake", "[1,)", 1, Set.of(), List.of());
    }

    @Override
    public ConnectionSchema schema() {
        return SCHEMA;
    }

    @Override
    public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        requests.add(new Request(cluster, credentials, context.options()));
        int number = requests.size();
        Transport transport = proxy(Transport.class, (self, method, args) -> switch (method.getName()) {
            case "watch" -> {
                watchers.add((LinkListener) args[0]);
                yield (AutoCloseable) () -> {};
            }
            default -> throw new UnsupportedOperationException(method.getName());
        });
        return proxy(ClusterConnection.class, (self, method, args) -> switch (method.getName()) {
            case "system" -> system();
            case "transport" -> messaging ? Optional.of(transport) : Optional.empty();
            case "close" -> {
                events.add("closed " + number + (driversOpen.getAsBoolean() ? " with drivers open" : " after drivers"));
                yield null;
            }
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        InvocationHandler withObjectMethods = (self, method, args) -> switch (method.getName()) {
            case "toString" -> type.getSimpleName() + " proxy";
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            default -> handler.invoke(self, method, args);
        };
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, withObjectMethods);
    }

    List<String> eventsCopy() {
        return new ArrayList<>(events);
    }
}
