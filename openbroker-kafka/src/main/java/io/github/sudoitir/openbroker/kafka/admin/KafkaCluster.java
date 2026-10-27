package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
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
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.SaslConfigs;
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

    private final CredentialSource credentials;
    private final DriverContext context;
    private volatile ClusterSpec spec;
    private @Nullable Admin admin;
    private boolean closed;

    public KafkaCluster(ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.spec = spec;
        this.credentials = credentials;
        this.context = context;
    }

    public ConnectOptions options() {
        return context.options();
    }

    /** Points the connection at a new cluster description; the next call uses a new Admin client. */
    public void reconfigure(ClusterSpec spec) {
        this.spec = spec;
        closeAdmin();
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
        config.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, (int)
                Math.min(options().timeouts().connect().toMillis(), Integer.MAX_VALUE));
        String protocol = current.option(KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.PLAINTEXT);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if (protocol.equals(KafkaOptions.SSL) || protocol.equals(KafkaOptions.SASL_SSL)) {
            if (options().tls().current().isPresent()) {
                throw KafkaErrors.unimplemented("use caller-supplied TLS material yet");
            }
            // Hostname verification stays on: the clients' default is to verify the broker's name.
        }
        if (protocol.startsWith("SASL_")) {
            String mechanism = current.option(KafkaOptions.SASL_MECHANISM, KafkaOptions.PLAIN);
            config.put(SaslConfigs.SASL_MECHANISM, mechanism);
            // A Password prints as [hidden], so the login never shows in a logged or printed config.
            config.put(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    new Password(Sasl.jaasConfig(mechanism, credentials.forPlane(plane))));
        }
        return config;
    }

    /**
     * Runs one call on the Admin client, admitted by the caller's interceptor, within {@code
     * deadline}.
     */
    public <T> T call(String operation, Optional<ResourceId> target, Deadline deadline, AdminCall<T> work) {
        ConnectOptions options = options();
        if (deadline.expired(options.clock())) {
            throw Deadlines.exceeded(CLUSTER, Dispatch.NOT_SENT);
        }
        String servers = bootstrapServers();
        try (Permit permit =
                options.interceptor().admit(new CallInfo(CLUSTER, servers, Plane.MANAGEMENT, operation, 1, target))) {
            try {
                T result = work.run(admin(), new Await(deadline, options.clock(), CLUSTER));
                permit.complete(Outcome.succeeded(), Optional.empty());
                return result;
            } catch (RuntimeException e) {
                BrokerException failure = KafkaErrors.of(e, CLUSTER);
                permit.complete(Outcome.failed(failure), Optional.empty());
                throw failure;
            }
        }
    }

    private synchronized Admin admin() {
        if (closed) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION, ErrorReasons.UNREACHABLE, "The connection is closed.");
        }
        if (admin == null) {
            admin = Admin.create(clientConfig(Plane.MANAGEMENT));
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
