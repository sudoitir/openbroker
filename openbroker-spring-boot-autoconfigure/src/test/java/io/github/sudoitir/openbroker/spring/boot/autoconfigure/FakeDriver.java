package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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
    /** What {@code nodes().describe(...)} does: it may answer, throw or block. */
    Supplier<Partial<List<NodeDescription>>> describe = () -> new Partial<>(List.of(), Map.of(), Map.of());
    /** What the n-th connection (from 1) does instead of {@link #describe}. */
    final Map<Integer, Supplier<Partial<List<NodeDescription>>>> describeOf = new ConcurrentHashMap<>();

    boolean messaging = true;
    private final BrokerSystem system;
    private final ConnectionSchema schema;

    FakeDriver() {
        this(new BrokerSystem(SYSTEM, "Fake", "[1,)", 1, Set.of(), List.of()), SCHEMA);
    }

    /** A driver that answers for another system, with that system's description and schema. */
    FakeDriver(BrokerSystem system, ConnectionSchema schema) {
        this.system = system;
        this.schema = schema;
    }

    @Override
    public BrokerSystem system() {
        return system;
    }

    @Override
    public ConnectionSchema schema() {
        return schema;
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
            case "process" -> {
                events.add("process started");
                yield proxy(
                        Processor.class,
                        (processorSelf, processorMethod, processorArgs) -> switch (processorMethod.getName()) {
                            case "stop" -> {
                                events.add("processor stopped within " + processorArgs[0]);
                                yield null;
                            }
                            case "close" -> null;
                            default -> throw new UnsupportedOperationException(processorMethod.getName());
                        });
            }
            default -> throw new UnsupportedOperationException(method.getName());
        });
        return proxy(ClusterConnection.class, (self, method, args) -> switch (method.getName()) {
            case "system" -> system();
            case "transport" -> messaging ? Optional.of(transport) : Optional.empty();
            case "nodes" ->
                proxy(Nodes.class, (nodesSelf, nodesMethod, nodesArgs) -> switch (nodesMethod.getName()) {
                    case "describe" -> describeOf.getOrDefault(number, describe).get();
                    default -> throw new UnsupportedOperationException(nodesMethod.getName());
                });
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
