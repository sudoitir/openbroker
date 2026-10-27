package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import java.util.Map;
import java.util.function.Supplier;

/** A call to the cluster as a whole, reported as the one node it is: it applied, or failed with why. */
final class Outcomes {

    private Outcomes() {}

    /** The work's value on success; {@code onFailure} and the failure, attributed to the cluster, otherwise. */
    static <T> Partial<T> of(T onFailure, Supplier<T> work) {
        try {
            return new Partial<>(work.get(), Map.of(KafkaCluster.CLUSTER, NodeOutcome.APPLIED), Map.of());
        } catch (BrokerException e) {
            BrokerException failure = e.at(KafkaCluster.CLUSTER);
            return new Partial<>(
                    onFailure,
                    Map.of(
                            KafkaCluster.CLUSTER,
                            failure.dispatch() == Dispatch.IN_DOUBT ? NodeOutcome.IN_DOUBT : NodeOutcome.FAILED),
                    Map.of(KafkaCluster.CLUSTER, failure));
        }
    }
}
