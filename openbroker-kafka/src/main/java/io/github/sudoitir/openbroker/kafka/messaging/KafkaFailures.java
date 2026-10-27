package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaErrors;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.errors.AuthorizationException;

/** The failures of the messaging surface, as the SDK's statuses, reasons and dispatch. */
final class KafkaFailures {

    static final String DATA = "DATA";

    private KafkaFailures() {}

    /** The SDK's failure for what a client threw while working on {@code resource}. */
    static BrokerException of(Throwable failure, Optional<ResourceId> resource) {
        BrokerException mapped = KafkaErrors.of(failure, KafkaCluster.CLUSTER);
        if (!(failure instanceof AuthorizationException) && resource.isEmpty()) {
            return mapped;
        }
        Map<String, String> metadata = new LinkedHashMap<>(mapped.info().metadata());
        metadata.put(ErrorReasons.PLANE, DATA);
        resource.ifPresent(r -> metadata.put(ErrorReasons.RESOURCE, r.format()));
        return new BrokerException(
                mapped.status(),
                ErrorInfo.of(mapped.reason(), metadata),
                mapped.dispatch(),
                mapped.node().orElse(null),
                mapped.getMessage(),
                failure);
    }

    static BrokerException invalid(String message) {
        return refused(Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, message);
    }

    static BrokerException precondition(String message) {
        return refused(Status.FAILED_PRECONDITION, ErrorReasons.PRECONDITION_FAILED, message);
    }

    static BrokerException refused(Status status, String reason, String message) {
        return KafkaErrors.refused(status, reason, message);
    }

    /** The settlement of a record whose partition is no longer this consumer's. */
    static BrokerException stale(String message) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.DELIVERY_STALE),
                Dispatch.NOT_SENT,
                KafkaCluster.CLUSTER,
                message);
    }
}
