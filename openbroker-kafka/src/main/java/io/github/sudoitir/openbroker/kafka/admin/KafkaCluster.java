package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.security.CredentialCallbackHandler;
import io.github.sudoitir.openbroker.kafka.security.TlsEngineFactory;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.jspecify.annotations.Nullable;

/**
 * What every facet of one connection shares: the cluster's bootstrap servers, the credentials, the
 * caller's options, and the Admin client, made on first use. Thread-safe.
 *
 * <p>The cluster is addressed as a whole, so a call is attributed to the node {@link #CLUSTER}. The
 * Admin client logs in with the {@code MANAGEMENT} plane's credentials; the producers and consumers
 * of the messaging surface build their configuration with {@link #clientConfig} for the {@code DATA}
 * plane's.
 */
public final class KafkaCluster implements AutoCloseable {

    /** The node a call to the cluster as a whole is attributed to. */
    public static final NodeId CLUSTER = NodeId.of("cluster");

    private static final Duration CLOSE_WAIT = Duration.ofSeconds(2);
    private static final long RECONNECT_BACKOFF_MS = 200;
    private static final long RECONNECT_BACKOFF_MAX_MS = 2000;

    private final CredentialSource credentials;
    private final DriverContext context;
    private final Function<Map<String, Object>, Admin> adminFactory;
    private volatile ClusterSpec spec;
    private @Nullable Admin admin;
    private final AtomicReference<@Nullable ClusterFeatures> features = new AtomicReference<>();
    private boolean closed;

    public KafkaCluster(ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this(spec, credentials, context, Admin::create);
    }

    /** Like the connection's, with the Admin client made by {@code adminFactory}: a test counts its calls. */
    public KafkaCluster(
            ClusterSpec spec,
            CredentialSource credentials,
            DriverContext context,
            Function<Map<String, Object>, Admin> adminFactory) {
        this.spec = spec;
        this.credentials = credentials;
        this.context = context;
        this.adminFactory = adminFactory;
    }

    public ConnectOptions options() {
        return context.options();
    }

    /** How often the watch reads the cluster, and so how old the shared topic description may be. */
    public Duration watchInterval() {
        String text = spec.option(KafkaOptions.WATCH_INTERVAL, KafkaOptions.DEFAULT_WATCH_INTERVAL);
        try {
            Duration interval = Duration.parse(text);
            if (interval.compareTo(Duration.ofSeconds(1)) < 0) {
                throw new IllegalArgumentException(text);
            }
            return interval;
        } catch (RuntimeException e) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    KafkaOptions.WATCH_INTERVAL + " is an ISO-8601 duration of one second or more, such as PT10S.");
        }
    }

    /** Points the connection at a new cluster description; the next call uses a new Admin client. */
    public void reconfigure(ClusterSpec spec) {
        this.spec = spec;
        features.set(null);
        closeAdmin();
    }

    /** What the cluster is and offers, read on first use and kept until the connection is pointed elsewhere. */
    public ClusterFeatures features() {
        ClusterFeatures known = features.get();
        return known != null ? known : readFeatures();
    }

    /** Reads what the cluster is and offers again, and keeps it. */
    public ClusterFeatures readFeatures() {
        return call("features.read", Optional.empty(), Deadlines.current(options()), (admin, await) -> {
            ClusterFeatures read = ClusterFeatures.read(admin, await);
            features.set(read);
            return read;
        });
    }

    /** The cluster's features, read with {@code admin} for a call already admitted when not yet known. */
    ClusterFeatures features(Admin admin, Await await) {
        ClusterFeatures known = features.get();
        if (known == null) {
            known = ClusterFeatures.read(admin, await);
            features.set(known);
        }
        return known;
    }

    /** The nodes the caller named, as they are known without asking the cluster: the nodes of the spec, else its endpoints. */
    public List<NodeSpec> declaredNodes() {
        ClusterSpec current = spec;
        if (!current.nodes().isEmpty()) {
            return current.nodes();
        }
        List<NodeSpec> out = new ArrayList<>();
        for (Endpoint e : current.clusterEndpoints()) {
            if (e.plane() == Plane.DATA && e.protocol().equals(KafkaOptions.PROTOCOL)) {
                out.add(new NodeSpec(NodeId.of("bootstrap-" + (out.size() + 1)), List.of(e)));
            }
        }
        return out;
    }

    /** The bootstrap servers: the {@code kafka} endpoints of the data plane, the cluster's first. */
    public String bootstrapServers() {
        Set<String> servers = new LinkedHashSet<>();
        for (Endpoint e : spec.clusterEndpoints()) {
            addBootstrap(servers, e);
        }
        for (NodeSpec n : spec.nodes()) {
            n.endpoints().forEach(e -> addBootstrap(servers, e));
        }
        if (servers.isEmpty()) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.ENDPOINT_NOT_FOUND,
                    "A Kafka cluster is reached at bootstrap servers: give it a DATA endpoint with protocol "
                            + KafkaOptions.PROTOCOL
                            + ", such as broker-1:9092.");
        }
        return String.join(",", servers);
    }

    private static void addBootstrap(Set<String> servers, Endpoint e) {
        if (e.plane() == Plane.DATA && e.protocol().equals(KafkaOptions.PROTOCOL)) {
            servers.add(e.address());
        }
    }

    /**
     * The settings every Kafka client of this connection starts from: bootstrap servers, client id,
     * security and the login of {@code plane}'s credentials, and the connection and call timeouts.
     * The credentials are asked for again at every call, so a rotation reaches the next client.
     */
    public Map<String, Object> clientConfig(Plane plane) {
        ClusterSpec current = spec;
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        config.put(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                current.option(KafkaOptions.CLIENT_ID, KafkaOptions.DEFAULT_CLIENT_ID));
        long call = options().timeouts().call().toMillis();
        config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, (int) Math.min(call, Integer.MAX_VALUE));
        config.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Math.min(call, Integer.MAX_VALUE));
        // A broker that cannot be connected to is tried again after a growing pause, not in a tight loop.
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, RECONNECT_BACKOFF_MS);
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, RECONNECT_BACKOFF_MAX_MS);
        // Kafka charges a broker's name lookup and SASL login to its connection setup: the socket is open but the
        // channel is not built, so the node stays CONNECTING and this timeout closes it. A reverse DNS with no
        // zone behind it, or a login that reaches a policy server, is therefore a setup timeout unless the budget
        // covers it, and the client then never reads a single API version. The caller's connect timeout is the TCP
        // budget; the setup budget is that or Kafka's own 10 s default, whichever is longer.
        config.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, (int)
                Math.min(setupTimeout(options().timeouts().connect()).toMillis(), Integer.MAX_VALUE));
        String protocol = current.option(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.PLAINTEXT);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if (protocol.equals(KafkaOptions.SSL) || protocol.equals(KafkaOptions.SASL_SSL)) {
            // The engine factory asks the TLS provider for every new connection, and verifies the
            // broker's host name against its certificate.
            config.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TlsEngineFactory.class);
            config.put(TlsEngineFactory.TLS, options().tls());
        }
        if (protocol.startsWith("SASL_")) {
            String mechanism = current.option(KafkaOptions.SASL_MECHANISM, KafkaOptions.PLAIN);
            config.put(SaslConfigs.SASL_MECHANISM, mechanism);
            config.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(Sasl.loginModule(mechanism)));
            Sasl.requireForm(mechanism, credentials.forPlane(plane));
            // The handler asks the source for each authentication; the login holds no secret.
            Supplier<Credentials> source = () -> credentials.forPlane(plane);
            config.put(CredentialCallbackHandler.CREDENTIALS, source);
            config.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, CredentialCallbackHandler.class);
            if (mechanism.equals(KafkaOptions.OAUTHBEARER)) {
                config.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CredentialCallbackHandler.class);
            }
        }
        return config;
    }

    /**
     * The connection setup budget: the caller's TCP connect timeout, or {@link KafkaOptions#SETUP_GRACE} (Kafka's
     * own 10 s default) when that is longer, so a slow name lookup or login does not close a socket that is about
     * to answer. A cluster may raise or lower the floor with that option.
     */
    Duration connectionSetupTimeout() {
        return setupTimeout(options().timeouts().connect());
    }

    private Duration setupTimeout(Duration connect) {
        String configured = spec.option(KafkaOptions.SETUP_GRACE, KafkaOptions.DEFAULT_SETUP_GRACE);
        Duration grace;
        try {
            grace = Duration.parse(configured);
            if (grace.isNegative() || grace.isZero()) {
                throw new IllegalArgumentException(configured);
            }
        } catch (RuntimeException e) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    KafkaOptions.SETUP_GRACE + " is an ISO-8601 duration above zero, such as PT10S.");
        }
        return connect.compareTo(grace) > 0 ? connect : grace;
    }

    /**
     * Runs one call on the Admin client, admitted by the caller's interceptor, within {@code
     * deadline}.
     */
    public <T> T call(String operation, Optional<ResourceId> target, Deadline deadline, AdminCall<T> work) {
        return guarded(operation, Plane.MANAGEMENT, target, deadline, () -> withAdmin(deadline, work));
    }

    /**
     * Runs {@code work} on an Admin client of its own that bootstraps from {@code address} alone, within
     * {@code deadline}, and closes it. What it asks reaches that broker and no other, so a broker that
     * cannot be reached fails by itself instead of holding up the connection's shared client.
     */
    public <T> T withAdminAt(String address, Deadline deadline, AdminCall<T> work) {
        Map<String, Object> config = new LinkedHashMap<>(clientConfig(Plane.MANAGEMENT));
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, address);
        Admin own = adminFactory.apply(config);
        try {
            return work.run(own, new Await(deadline, options().clock(), CLUSTER));
        } finally {
            own.close(Duration.ofMillis(100));
        }
    }

    /** Runs {@code work} on the Admin client within {@code deadline}, for a call already admitted. */
    public <T> T withAdmin(Deadline deadline, AdminCall<T> work) {
        return work.run(admin(), new Await(deadline, options().clock(), CLUSTER));
    }

    /**
     * Runs {@code work} as one call to the cluster on {@code plane}: refused if the deadline has
     * passed, admitted by the caller's interceptor, and its failures reported as the SDK's.
     */
    public <T> T guarded(
            String operation, Plane plane, Optional<ResourceId> target, Deadline deadline, Supplier<T> work) {
        ConnectOptions options = options();
        if (deadline.expired(options.clock())) {
            throw Deadlines.exceeded(CLUSTER, Dispatch.NOT_SENT);
        }
        String servers = bootstrapServers();
        try (Permit permit = options.interceptor().admit(new CallInfo(CLUSTER, servers, plane, operation, 1, target))) {
            try {
                T result = work.get();
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (RuntimeException e) {
                BrokerException failure = KafkaErrors.of(e, CLUSTER);
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            }
        }
    }

    /**
     * A view of the connection's Admin client for callers that need Kafka's own API. It follows the
     * client across a reconfiguration, ignores {@code close}, and is not admitted by the caller's
     * interceptor: the connection owns the client.
     */
    public Admin nativeAdmin() {
        return (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    try {
                        return method.invoke(admin(), args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private synchronized Admin admin() {
        if (closed) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION, ErrorReasons.UNREACHABLE, "The connection is closed.");
        }
        if (admin == null) {
            Map<String, Object> config = new LinkedHashMap<>(clientConfig(Plane.MANAGEMENT));
            // A client given a name that does not resolve fails to build, and one given a closed port retries
            // until its deadline: it is given the servers that accept a connection, or the call fails here.
            config.put(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    String.join(
                            ",",
                            Bootstrap.reachable(
                                    List.of(bootstrapServers().split(",")),
                                    options().timeouts().connect(),
                                    options().clock())));
            admin = adminFactory.apply(config);
        }
        return admin;
    }

    private synchronized void closeAdmin() {
        if (admin != null) {
            admin.close(CLOSE_WAIT);
            admin = null;
        }
    }

    @Override
    public void close() {
        closeAdmin();
        synchronized (this) {
            closed = true;
        }
    }

    /** One unit of work on the Admin client. */
    @FunctionalInterface
    public interface AdminCall<T> {
        T run(Admin admin, Await await);
    }
}
