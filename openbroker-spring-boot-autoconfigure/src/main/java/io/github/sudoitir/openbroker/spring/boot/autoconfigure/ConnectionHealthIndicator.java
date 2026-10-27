package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * The health of one connection, from the cluster's own description of its nodes, taken on a virtual thread
 * within a deadline.
 *
 * <ul>
 *   <li>Up when the cluster answers. A partial answer counts: a cluster that serves must not flap
 *       readiness, and the details name each node that did not answer, with its status and reason.
 *   <li>Down when no node answers or the deadline passes, with the reason.
 *   <li>Unknown when no node has a management endpoint to ask, as with a connection derived from Spring's
 *       details: the cluster is neither shown up nor shown down.
 *   <li>Out of service while the connection closes.
 * </ul>
 *
 * <p>At most one check runs. While one is in flight the last result comes back marked stale. The details
 * carry counts, node ids, statuses and reasons, never endpoints, options or credentials.
 */
final class ConnectionHealthIndicator implements HealthIndicator {

    private final ClusterConnection connection;
    private final Duration timeout;
    private final AtomicReference<@Nullable CompletableFuture<Health>> running = new AtomicReference<>();
    private volatile @Nullable Health last;
    private volatile boolean closing;

    ConnectionHealthIndicator(ClusterConnection connection, Duration timeout) {
        this.connection = connection;
        this.timeout = timeout;
    }

    /** The connection is closing: health is out of service from now on. */
    void closing() {
        closing = true;
    }

    @Override
    public Health health() {
        if (closing) {
            return Health.outOfService().build();
        }
        CompletableFuture<Health> check = new CompletableFuture<>();
        if (!running.compareAndSet(null, check)) {
            return stale(last);
        }
        Thread.ofVirtual().name("openbroker-health").start(() -> finish(check));
        try {
            Health result = check.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            last = result;
            return result;
        } catch (TimeoutException e) {
            return remember(Health.down()
                    .withDetail("reason", "TIMEOUT")
                    .withDetail("timeout", timeout.toString())
                    .build());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("reason", "INTERRUPTED").build();
        } catch (ExecutionException e) {
            return remember(down(e.getCause()));
        }
    }

    private Health remember(Health health) {
        last = health;
        return health;
    }

    private void finish(CompletableFuture<Health> check) {
        try {
            check.complete(probe());
        } catch (Exception e) {
            check.complete(down(e));
        } finally {
            running.set(null);
        }
    }

    private Health probe() {
        Partial<List<NodeDescription>> described = connection.nodes().describe(Scope.whole());
        Map<String, Object> failed = new TreeMap<>();
        int answered = 0;
        for (Map.Entry<NodeId, NodeOutcome> outcome : described.outcomes().entrySet()) {
            if (outcome.getValue() == NodeOutcome.APPLIED) {
                answered++;
            } else {
                failed.put(
                        outcome.getKey().value(),
                        failure(outcome.getValue(), described.failures().get(outcome.getKey())));
            }
        }
        described
                .failures()
                .forEach((node, error) -> failed.putIfAbsent(node.value(), failure(NodeOutcome.FAILED, error)));
        Health.Builder health = answered > 0 ? Health.up() : unmanaged(described) ? Health.unknown() : Health.down();
        health.withDetail("nodes", answered + failed.size()).withDetail("answered", answered);
        if (!failed.isEmpty()) {
            health.withDetail("failed", failed);
        }
        if (answered == 0 && failed.isEmpty()) {
            health.withDetail("reason", "NO_NODES");
        } else if (answered == 0 && unmanaged(described)) {
            health.withDetail("reason", ErrorReasons.ENDPOINT_NOT_FOUND);
        }
        return health.build();
    }

    /**
     * Whether no node could be asked because none has a management endpoint, as with a connection derived
     * from Spring's details, which state only the data endpoint. That is not an answer either way.
     */
    private static boolean unmanaged(Partial<?> described) {
        return !described.failures().isEmpty()
                && described.failures().values().stream()
                        .allMatch(error -> error.reason().equals(ErrorReasons.ENDPOINT_NOT_FOUND));
    }

    private static Map<String, String> failure(NodeOutcome outcome, @Nullable BrokerException error) {
        Map<String, String> detail = new TreeMap<>();
        detail.put("outcome", outcome.name());
        if (error != null) {
            detail.put("status", error.status().name());
            detail.put("reason", error.reason());
        }
        return detail;
    }

    private static Health down(@Nullable Throwable error) {
        Health.Builder health = Health.down();
        if (error instanceof BrokerException broker) {
            health.withDetail("status", broker.status().name()).withDetail("reason", broker.reason());
        } else {
            health.withDetail("reason", "ERROR");
            if (error != null) {
                health.withDetail("error", error.getClass().getSimpleName());
            }
        }
        return health.build();
    }

    private static Health stale(@Nullable Health previous) {
        Health.Builder health = previous == null
                ? Health.down().withDetail("reason", "CHECK_IN_PROGRESS")
                : Health.status(previous.getStatus()).withDetails(previous.getDetails());
        return health.withDetail("stale", true).build();
    }
}
