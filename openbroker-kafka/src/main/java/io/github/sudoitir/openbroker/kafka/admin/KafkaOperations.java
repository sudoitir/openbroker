package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.Optional;

/**
 * Operations on Kafka resources. A run that names a preview's etag is refused when the state the
 * operation acts on has changed since; it is checked before the change, because Kafka offers no
 * conditional write, and the cluster's own refusals (a group that gained members) are the backstop.
 */
public final class KafkaOperations implements Operations {

    private final KafkaCluster cluster;
    private final Handlers handlers;

    KafkaOperations(KafkaCluster cluster, Handlers handlers) {
        this.cluster = cluster;
        this.handlers = handlers;
    }

    @Override
    public Preview preview(ResourceId resource, Operation operation, Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return handlers.call(
                "operations.preview",
                Optional.of(resource),
                deadline,
                context -> handlers.of(context, resource.kind()).preview(context, resource, operation, scope));
    }

    @Override
    public Partial<OperationResult> execute(
            ResourceId resource, Operation operation, Scope scope, Precondition precondition) {
        Deadline deadline = Deadlines.current(cluster.options());
        return Outcomes.of(
                OperationResult.done(),
                () -> handlers.call("operations.execute", Optional.of(resource), deadline, context -> {
                    Handler handler = handlers.of(context, resource.kind());
                    if (precondition instanceof Precondition.IfMatch match
                            && !match.etag().equals(handler.etag(context, resource, operation, scope))) {
                        throw KafkaErrors.refused(
                                Status.FAILED_PRECONDITION,
                                ErrorReasons.PRECONDITION_FAILED,
                                resource + " changed since the preview; preview it again.");
                    }
                    return handler.execute(context, resource, operation, scope);
                }));
    }
}
