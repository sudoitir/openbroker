package io.github.sudoitir.openbroker.operation;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * What an operation would do, without doing it: Terraform's plan.
 *
 * @param diff the attributes it would change
 * @param requiresReplace the changed attributes that need the resource deleted and created again
 * @param estimate how much it would affect
 * @param etag the resource's identity over the resolved scope, in the AIP-154 sense: pass it to
 *     {@code Precondition.ifMatch} so the operation runs only on what was previewed. It covers the
 *     shard set and each shard's incarnation, not volume, so a resource recreated since, or a node
 *     that joined, refuses the operation, while a backlog that grew does not
 * @param enforcement how the precondition is enforced
 */
public record Preview(
        List<AttributeChange> diff,
        Set<String> requiresReplace,
        Estimate estimate,
        String etag,
        PreconditionEnforcement enforcement) {

    public Preview {
        diff = List.copyOf(diff);
        requiresReplace = Set.copyOf(requiresReplace);
        Objects.requireNonNull(estimate, "estimate");
        Objects.requireNonNull(etag, "etag");
        Objects.requireNonNull(enforcement, "enforcement");
    }
}
