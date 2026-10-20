package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Runs one call per node, each on its own virtual thread so a slow node holds nothing but itself, and
 * gathers the answers into a {@link Partial}. A single node is called on the caller's thread.
 */
public final class Fanout {

    private Fanout() {}

    public static <T> Partial<List<T>> each(
            List<NodeHandle> nodes, ConnectOptions options, Function<NodeHandle, List<T>> call) {
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        List<T> values = new ArrayList<>();
        if (nodes.size() == 1) {
            NodeHandle only = nodes.getFirst();
            try {
                values.addAll(call.apply(only));
                outcomes.put(only.id(), NodeOutcome.APPLIED);
            } catch (BrokerException e) {
                record(only.id(), e, outcomes, failures);
            }
            return new Partial<>(values, outcomes, failures);
        }
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<NodeHandle, Future<List<T>>> futures = new LinkedHashMap<>();
            for (NodeHandle node : nodes) {
                futures.put(node, pool.submit(() -> {
                    List<T>[] out = new List[1];
                    options.contextPropagation().apply(() -> out[0] = call.apply(node)).run();
                    return out[0];
                }));
            }
            for (Map.Entry<NodeHandle, Future<List<T>>> e : futures.entrySet()) {
                NodeId id = e.getKey().id();
                try {
                    values.addAll(e.getValue().get());
                    outcomes.put(id, NodeOutcome.APPLIED);
                } catch (ExecutionException ex) {
                    BrokerException failure = ex.getCause() instanceof BrokerException b
                            ? b
                            : new BrokerException(Status.INTERNAL, ErrorInfo.of(ErrorReasons.BAD_RESPONSE),
                                    Dispatch.IN_DOUBT, id, String.valueOf(ex.getCause()), ex.getCause());
                    record(id, failure, outcomes, failures);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw Deadlines.cancelled(id, ex);
                }
            }
        }
        return new Partial<>(values, outcomes, failures);
    }

    private static void record(
            NodeId id, BrokerException e, Map<NodeId, NodeOutcome> outcomes, Map<NodeId, BrokerException> failures) {
        BrokerException at = e.node().isPresent() ? e : e.at(id);
        outcomes.put(id, at.dispatch() == Dispatch.IN_DOUBT ? NodeOutcome.IN_DOUBT : NodeOutcome.FAILED);
        failures.put(id, at);
    }
}
