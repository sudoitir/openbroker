package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;

/** {@link Operations}: previews and executes the verbs of each kind, with etags that name the state acted on. */
final class RabbitMqOperations implements Operations {

    RabbitMqOperations(RabbitMqCluster cluster) {}

    @Override
    public Preview preview(ResourceId resource, Operation operation, Scope scope) {
        throw RabbitMqErrors.unimplemented("RabbitMqOperations.preview");
    }

    @Override
    public Partial<OperationResult> execute(
            ResourceId resource, Operation operation, Scope scope, Precondition precondition) {
        throw RabbitMqErrors.unimplemented("RabbitMqOperations.execute");
    }
}
