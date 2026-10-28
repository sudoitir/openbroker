package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Whether every broker can be reached on the address it advertises. The cluster answers a
 * description through whichever broker the bootstrap servers lead to, and every broker advertises
 * the address clients must use for it, which can differ from the bootstrap address and be one the
 * caller cannot reach. So each broker is checked on its own, in parallel and within one budget: its
 * name must resolve, a connection to it must open, and it must answer a request addressed to it. A
 * broker that fails any step is a failed node that names the step; the other nodes keep their
 * descriptions.
 */
final class Reach {

    /** The metadata key of the address a broker advertises. */
    static final String ENDPOINT = "kafka.endpoint";

    /** The metadata key of the step that failed: {@code dns}, {@code refused}, {@code timeout}, {@code tls} or {@code authentication}. */
    static final String FAILURE = "kafka.reach_failure";

    private static final String TIMEOUT = "timeout";

    /** What the checks may overrun the budget by before they are abandoned. */
    private static final Duration GRACE = Duration.ofMillis(250);

    private final KafkaCluster cluster;
    private final Clock clock;

    /** The request addressed to one broker; it throws what the cluster answered with. */
    @FunctionalInterface
    interface Ask {
        void to(NodeDescription node, Deadline limit);
    }

    private final Ask ask;

    private Reach(KafkaCluster cluster, Ask ask) {
        this.cluster = cluster;
        this.clock = cluster.options().clock();
        this.ask = ask;
    }

    /**
     * The nodes with each broker's outcome: applied when it was reached, failed with why when not,
     * and skipped for a node that is no broker.
     */
    static Partial<List<NodeDescription>> of(
            KafkaCluster cluster, Partial<List<NodeDescription>> described, Deadline deadline) {
        Reach reach = new Reach(cluster, null);
        return new Reach(cluster, reach::describeConfigs).check(described, deadline);
    }

    /** Like {@link #of}, asking each broker through {@code ask}. */
    static Partial<List<NodeDescription>> of(
            KafkaCluster cluster, Partial<List<NodeDescription>> described, Deadline deadline, Ask ask) {
        return new Reach(cluster, ask).check(described, deadline);
    }

    private Partial<List<NodeDescription>> check(Partial<List<NodeDescription>> described, Deadline deadline) {
        Duration budget = min(cluster.options().timeouts().connect(), deadline.remaining(clock));
        Deadline limit = Deadline.after(budget, clock);
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>(described.outcomes());
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>(described.failures());
        Map<NodeDescription, Future<Optional<BrokerException>>> running = new LinkedHashMap<>();
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (NodeDescription node : described.value()) {
                if (node.functions().contains(NodeFunction.BROKER)) {
                    running.put(node, pool.submit(() -> reach(node, limit)));
                } else {
                    outcomes.put(node.node(), NodeOutcome.SKIPPED);
                }
            }
            long give = System.nanoTime() + budget.plus(GRACE).toNanos();
            for (Map.Entry<NodeDescription, Future<Optional<BrokerException>>> e : running.entrySet()) {
                NodeDescription node = e.getKey();
                Optional<BrokerException> failure =
                        await(node, e.getValue(), Math.max(0, give - System.nanoTime()), budget);
                if (failure.isPresent()) {
                    outcomes.put(node.node(), NodeOutcome.FAILED);
                    failures.put(node.node(), failure.get());
                }
            }
        } finally {
            // A name lookup cannot be interrupted, so the pool is not waited for.
            pool.shutdownNow();
        }
        return new Partial<>(described.value(), outcomes, failures);
    }

    private static Optional<BrokerException> await(
            NodeDescription node, Future<Optional<BrokerException>> future, long nanos, Duration budget) {
        try {
            return future.get(nanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException _) {
            future.cancel(true);
            return Optional.of(unreached(node, TIMEOUT, "did not answer within " + seconds(budget)));
        } catch (ExecutionException | CancellationException e) {
            return Optional.of(unreached(node, TIMEOUT, "could not be checked: " + e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.of(Deadlines.cancelled(node.node(), e));
        }
    }

    /** Checks one broker; empty when it was reached. */
    private Optional<BrokerException> reach(NodeDescription node, Deadline limit) {
        Optional<Endpoint> advertised = node.endpoints().stream()
                .filter(e -> e.plane() == Plane.DATA && e.protocol().equals(KafkaOptions.PROTOCOL))
                .findFirst();
        if (advertised.isEmpty()) {
            return Optional.empty();
        }
        String address = advertised.get().address();
        int colon = address.lastIndexOf(':');
        String host = unbracketed(address.substring(0, colon));
        int port = Integer.parseInt(address.substring(colon + 1));
        InetAddress[] resolved;
        try {
            resolved = InetAddress.getAllByName(host);
        } catch (UnknownHostException _) {
            return Optional.of(unreached(node, "dns", "does not resolve"));
        }
        Optional<BrokerException> connect = connect(node, resolved, port, limit);
        if (connect.isPresent()) {
            return connect;
        }
        return answers(node, limit);
    }

    /** An IPv6 literal without the brackets an address writes it in. */
    static String unbracketed(String host) {
        return host.length() > 1 && host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1)
                : host;
    }

    private Optional<BrokerException> connect(NodeDescription node, InetAddress[] resolved, int port, Deadline limit) {
        boolean timedOut = false;
        for (InetAddress address : resolved) {
            int millis = Math.clamp(limit.remaining(clock).toMillis(), 1, Integer.MAX_VALUE);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(address, port), millis);
                return Optional.empty();
            } catch (ConnectException _) {
                // Refused; the next address may answer.
            } catch (IOException _) {
                // A timeout or any other failure to open the connection.
                timedOut = true;
            }
        }
        return Optional.of(
                timedOut
                        ? unreached(node, TIMEOUT, "did not accept a connection in time")
                        : unreached(node, "refused", "refuses connections"));
    }

    /** Sends the broker a request addressed to it, which a half-open connection does not answer. */
    private Optional<BrokerException> answers(NodeDescription node, Deadline limit) {
        try {
            ask.to(node, limit);
            return Optional.empty();
        } catch (BrokerException e) {
            if (e.reason().equals(ErrorReasons.TLS_HANDSHAKE_FAILED)) {
                return Optional.of(named(node, "tls", e));
            }
            if (e.reason().equals(ErrorReasons.CREDENTIALS_REJECTED)) {
                return Optional.of(named(node, "authentication", e));
            }
            if (e.reason().equals(ErrorReasons.UNREACHABLE) || e.status() == Status.DEADLINE_EXCEEDED) {
                return Optional.of(unreached(node, TIMEOUT, "accepted a connection but did not answer in time"));
            }
            // Any other answer, a denial included, is a broker that was reached.
            return Optional.empty();
        }
    }

    private void describeConfigs(NodeDescription node, Deadline limit) {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, node.nativeId());
        cluster.withAdmin(
                limit,
                (admin, await) -> await.get(admin.describeConfigs(
                                List.of(broker), new DescribeConfigsOptions().timeoutMs(await.timeoutMs()))
                        .all()));
    }

    private static BrokerException named(NodeDescription node, String failure, BrokerException cause) {
        Map<String, String> metadata = new LinkedHashMap<>(cause.info().metadata());
        metadata.put(ENDPOINT, endpoint(node));
        metadata.put(FAILURE, failure);
        return new BrokerException(
                cause.status(),
                ErrorInfo.of(cause.reason(), metadata),
                Dispatch.NOT_SENT,
                node.node(),
                "Broker " + node.nativeId() + " at " + endpoint(node) + ": " + cause.getMessage(),
                cause);
    }

    private static BrokerException unreached(NodeDescription node, String failure, String what) {
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.UNREACHABLE, Map.of(ENDPOINT, endpoint(node), FAILURE, failure)),
                Dispatch.NOT_SENT,
                node.node(),
                "Broker " + node.nativeId() + " advertises " + endpoint(node) + ", which " + what + ".");
    }

    private static String endpoint(NodeDescription node) {
        List<String> addresses = new ArrayList<>();
        node.endpoints().forEach(e -> addresses.add(e.address()));
        return addresses.isEmpty() ? "no address" : addresses.get(0);
    }

    private static Duration min(Duration a, Duration b) {
        Duration least = a.compareTo(b) <= 0 ? a : b;
        return least.isNegative() || least.isZero() ? Duration.ofMillis(1) : least;
    }

    private static String seconds(Duration d) {
        return d.toMillis() < 1000 ? d.toMillis() + " ms" : d.toSeconds() + " s";
    }
}
