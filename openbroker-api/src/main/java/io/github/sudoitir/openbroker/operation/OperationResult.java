package io.github.sudoitir.openbroker.operation;

import io.github.sudoitir.openbroker.model.Resource;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * What an operation did on the nodes it applied to.
 *
 * @param affected how much it affected, in the unit its preview named, where the system says
 * @param resource the resource as it is now, where the operation returns it
 * @param alreadyInState the resource was already in the requested state, so nothing changed
 */
public record OperationResult(OptionalLong affected, Optional<Resource> resource, boolean alreadyInState) {

    public OperationResult {
        Objects.requireNonNull(affected, "affected");
        Objects.requireNonNull(resource, "resource");
    }

    public static OperationResult done() {
        return new OperationResult(OptionalLong.empty(), Optional.empty(), false);
    }

    public static OperationResult affected(long count) {
        return new OperationResult(OptionalLong.of(count), Optional.empty(), false);
    }

    public static OperationResult already() {
        return new OperationResult(OptionalLong.empty(), Optional.empty(), true);
    }
}
