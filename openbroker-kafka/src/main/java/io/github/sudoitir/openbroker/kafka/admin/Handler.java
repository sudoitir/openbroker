package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** How one kind is listed, read and operated on. */
interface Handler {

    Kind kind();

    /** The fields a query's selector may name, besides attributes the kind lists. */
    Set<String> selectors();

    /** The resources that match the listing, sorted by id, without their details. */
    List<Candidate> candidates(Context context, Listing listing);

    /** The details of the candidates of one page. */
    List<Resource> resources(Context context, List<Candidate> page, Listing listing);

    /** One resource by id, or none if it does not exist. */
    Optional<Resource> find(Context context, ResourceId id);

    default Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        throw unsupported(operation);
    }

    default OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        throw unsupported(operation);
    }

    /** The fingerprint of the state {@code operation} acts on; a run against an older fingerprint is refused. */
    default String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        Resource resource = find(context, id).orElseThrow(() -> gone(id));
        return Etags.of(id.format(), resource.attributes().asMap());
    }

    default BrokerException unsupported(Operation operation) {
        return KafkaErrors.refused(
                Status.UNIMPLEMENTED,
                ErrorReasons.NOT_SUPPORTED,
                "Kafka does not " + operation.getClass().getSimpleName().toLowerCase() + " a "
                        + kind().label().singular().toLowerCase() + ".");
    }

    static BrokerException gone(ResourceId id) {
        return KafkaErrors.refused(Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, id + " does not exist.");
    }
}
