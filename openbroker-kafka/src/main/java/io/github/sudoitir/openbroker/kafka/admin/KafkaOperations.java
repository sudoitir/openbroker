package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;

/** Operations on Kafka resources. */
public final class KafkaOperations implements Operations {

    private final KafkaCluster cluster;

    public KafkaOperations(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Preview preview(ResourceId resource, Operation operation, Scope scope) {
        throw KafkaErrors.unimplemented("run operations yet");
    }

    @Override
    public Partial<OperationResult> execute(
            ResourceId resource, Operation operation, Scope scope, Precondition precondition) {
        throw KafkaErrors.unimplemented("run operations yet");
    }
}
