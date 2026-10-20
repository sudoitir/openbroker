package io.github.sudoitir.openbroker;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A result that spans nodes and may be partial: the value gathered from the nodes that answered,
 * what the call did on each node, and why each failed node failed. No rollback is attempted for a
 * write that applied on some nodes and not on others.
 */
public record Partial<T>(T value, Map<NodeId, NodeOutcome> outcomes, Map<NodeId, BrokerException> failures) {

    public Partial {
        Objects.requireNonNull(value, "value");
        outcomes = Map.copyOf(outcomes);
        failures = Map.copyOf(failures);
    }

    /** A value every listed node contributed to. */
    public static <T> Partial<T> complete(T value, Iterable<NodeId> nodes) {
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        nodes.forEach(n -> outcomes.put(n, NodeOutcome.APPLIED));
        return new Partial<>(value, outcomes, Map.of());
    }

    public boolean isComplete() {
        return failures.isEmpty() && outcomes.values().stream().allMatch(o -> o == NodeOutcome.APPLIED);
    }

    public <R> Partial<R> map(Function<? super T, ? extends R> mapper) {
        return new Partial<>(mapper.apply(value), outcomes, failures);
    }

    /** The value, or the first failure when no node answered at all. */
    public T orThrow() {
        if (!failures.isEmpty() && outcomes.values().stream().noneMatch(o -> o == NodeOutcome.APPLIED)) {
            throw failures.values().iterator().next();
        }
        return value;
    }
}
