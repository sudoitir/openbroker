package io.github.sudoitir.openbroker.operation;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;

/** Previewing and executing operations. */
public interface Operations {

    /** What {@code operation} on {@code resource} over {@code scope} would do. */
    Preview preview(ResourceId resource, Operation operation, Scope scope);

    /**
     * Executes {@code operation} on {@code resource} over {@code scope} under {@code precondition}. A
     * failed precondition fails with {@code FAILED_PRECONDITION} and reason {@code
     * PRECONDITION_FAILED}, on every node, before anything changes.
     */
    Partial<OperationResult> execute(ResourceId resource, Operation operation, Scope scope, Precondition precondition);

    default Partial<OperationResult> execute(ResourceId resource, Operation operation, Scope scope) {
        return execute(resource, operation, scope, Precondition.none());
    }
}
