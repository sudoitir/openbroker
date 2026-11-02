package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The per-connection context shared by the facets: spec, credentials, connect options, the {@code ManagementClient},
 * the cached cluster features and queue types. The AMQP connections register with {@link #onReconfigure} and
 * {@link #onClose}.
 *
 * <p>A management endpoint's address is a base URL, {@code http(s)://host:port} with an optional path prefix, whose
 * scheme matches the endpoint's protocol. A data endpoint's address is {@code host:port}, and the port defaults to
 * 5672 for {@code amqp} and 5671 for {@code amqps}.
 */
public final class RabbitMqCluster implements AutoCloseable {

    /** The node id of calls that concern the whole cluster. */
    public static final NodeId CLUSTER = NodeId.of("cluster");

    /** The port of {@code amqp} when the address names none. */
    public static final int AMQP_PORT = 5672;

    /** The port of {@code amqps} when the address names none. */
    public static final int AMQPS_PORT = 5671;

    /** A management endpoint resolved: the node it belongs to and its base URL without a trailing slash. */
    record Target(NodeId node, String protocol, URI base) {}

    /** A data endpoint resolved. */
    public record HostPort(String host, int port) {}

    /** The specification and the management endpoints resolved from it, swapped together on a reconfiguration. */
    private record Configuration(ClusterSpec spec, List<Target> targets) {}

    private final CredentialSource credentials;
    private final DriverContext context;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final List<Runnable> reconfigureListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> closeListeners = new CopyOnWriteArrayList<>();
    private final ManagementClient management;
    private final ClusterFeatures features;
    private final QueueTypes queueTypes;
    private final AtomicReference<Configuration> configuration;

    /**
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT}, not sent, when an endpoint
     *     address is malformed
     */
    public RabbitMqCluster(ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.credentials = credentials;
        this.context = context;
        this.configuration = new AtomicReference<>(new Configuration(spec, resolve(spec)));
        this.management = new ManagementClient(this);
        this.features = new ClusterFeatures(this);
        this.queueTypes = new QueueTypes(this);
    }

    public ClusterSpec spec() {
        return configuration.get().spec();
    }

    public ConnectOptions options() {
        return context.options();
    }

    public DriverContext context() {
        return context;
    }

    public CredentialSource credentials() {
        return credentials;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public ManagementClient management() {
        return management;
    }

    ClusterFeatures features() {
        return features;
    }

    QueueTypes queueTypes() {
        return queueTypes;
    }

    /** The management endpoints in the order they are tried: the cluster's, then each node's. */
    List<Target> managementTargets() {
        return configuration.get().targets();
    }

    /** Calls {@code listener} after every {@link #reconfigure}. */
    public void onReconfigure(Runnable listener) {
        reconfigureListeners.add(listener);
    }

    /** Calls {@code listener} on {@link #close}. */
    public void onClose(Runnable listener) {
        closeListeners.add(listener);
    }

    /** Runs {@code work}, refusing it first when {@code deadline} has passed. */
    public <T> T call(Deadline deadline, Supplier<T> work) {
        if (deadline.expired(options().clock())) {
            throw Deadlines.exceeded(CLUSTER, Dispatch.NOT_SENT);
        }
        return work.get();
    }

    /** Applies a changed specification: drops the caches and tells the AMQP connections. */
    public void reconfigure(ClusterSpec spec) {
        List<Target> resolved = resolve(spec);
        configuration.set(new Configuration(spec, resolved));
        features.reset();
        queueTypes.reset();
        reconfigureListeners.forEach(Runnable::run);
    }

    @Override
    public void close() {
        closeListeners.forEach(Runnable::run);
    }

    /** The host and port of a data endpoint's address. */
    public static HostPort dataAddress(Endpoint endpoint) {
        String address = endpoint.address().trim();
        int defaultPort = RabbitMqOptions.AMQPS.equals(endpoint.protocol()) ? AMQPS_PORT : AMQP_PORT;
        if (address.contains("://") || address.contains("/")) {
            throw RabbitMqErrors.invalid("A data endpoint's address is host:port, such as rabbit.example.com:"
                    + defaultPort + ", not '" + redact(address) + "'.");
        }
        HostText split =
                address.startsWith("[") && address.contains("]") ? bracketed(address) : plain(address, defaultPort);
        int port = split.portText() == null ? defaultPort : port(split.portText());
        if (split.host().isBlank()) {
            throw RabbitMqErrors.invalid("A data endpoint's address names a host.");
        }
        return new HostPort(split.host(), port);
    }

    /** A host and the text after its colon, if there is one. */
    private record HostText(String host, @Nullable String portText) {}

    private static HostText bracketed(String address) {
        int close = address.indexOf(']');
        String rest = address.substring(close + 1);
        if (rest.startsWith(":")) {
            return new HostText(address.substring(0, close + 1), rest.substring(1));
        }
        if (!rest.isEmpty()) {
            throw RabbitMqErrors.invalid("A data endpoint's address is host:port, not '" + address + "'.");
        }
        return new HostText(address.substring(0, close + 1), null);
    }

    private static HostText plain(String address, int defaultPort) {
        int colon = address.indexOf(':');
        if (colon < 0) {
            return new HostText(address, null);
        }
        if (address.indexOf(':', colon + 1) >= 0) {
            throw RabbitMqErrors.invalid("Put an IPv6 address in brackets: [::1]:" + defaultPort + ".");
        }
        return new HostText(address.substring(0, colon), address.substring(colon + 1));
    }

    private static int port(String portText) {
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException _) {
            throw RabbitMqErrors.invalid("The port of a data endpoint is a number, not '" + portText + "'.");
        }
        if (port < 1 || port > 65535) {
            throw RabbitMqErrors.invalid("The port of a data endpoint is 1 to 65535, not " + port + ".");
        }
        return port;
    }

    private static List<Target> resolve(ClusterSpec spec) {
        List<Target> out = new ArrayList<>();
        for (Endpoint endpoint : spec.clusterEndpoints()) {
            addManagement(out, CLUSTER, endpoint);
        }
        for (NodeSpec node : spec.nodes()) {
            for (Endpoint endpoint : node.endpoints()) {
                addManagement(out, node.id(), endpoint);
            }
        }
        for (Endpoint endpoint : spec.clusterEndpoints()) {
            validateData(endpoint);
        }
        spec.nodes().forEach(n -> n.endpoints().forEach(RabbitMqCluster::validateData));
        return List.copyOf(out);
    }

    private static void validateData(Endpoint endpoint) {
        if (endpoint.plane() == Plane.DATA) {
            dataAddress(endpoint);
        }
    }

    private static void addManagement(List<Target> out, NodeId node, Endpoint endpoint) {
        if (endpoint.plane() != Plane.MANAGEMENT) {
            return;
        }
        String protocol = endpoint.protocol();
        String address = endpoint.address().trim();
        String expected = protocol + "://";
        if (!address.regionMatches(true, 0, expected, 0, expected.length())) {
            throw RabbitMqErrors.invalid("A management endpoint's address is a base URL starting with " + expected
                    + " to match its protocol '" + protocol + "', such as " + expected
                    + "rabbit.example.com:15672, not '" + redact(address) + "'.");
        }
        URI base;
        try {
            base = new URI(address);
        } catch (URISyntaxException e) {
            throw RabbitMqErrors.invalid("The management endpoint address is not a valid URL: " + e.getReason() + ".");
        }
        if (base.getHost() == null) {
            throw RabbitMqErrors.invalid("The management endpoint address names no host.");
        }
        if (base.getUserInfo() != null) {
            throw RabbitMqErrors.invalid(
                    "A management endpoint's address holds no user information; give credentials separately.");
        }
        String path = base.getRawPath() == null ? "" : withoutTrailingSlashes(base.getRawPath());
        String root = base.getScheme() + "://" + base.getRawAuthority() + path;
        out.add(new Target(node, protocol, URI.create(root)));
    }

    private static String withoutTrailingSlashes(String path) {
        int end = path.length();
        while (end > 0 && path.charAt(end - 1) == '/') {
            end--;
        }
        return path.substring(0, end);
    }

    /** An address without anything before an {@code @}, so a mistaken user:password never reaches a message. */
    private static String redact(String address) {
        int at = address.lastIndexOf('@');
        return at < 0 ? address : "***" + address.substring(at);
    }
}
