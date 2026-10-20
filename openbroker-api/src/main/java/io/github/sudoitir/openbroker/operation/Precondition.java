package io.github.sudoitir.openbroker.operation;

import java.util.Objects;

/** A condition an operation runs under, as part of the request, as in Kubernetes. */
public sealed interface Precondition {

    record None() implements Precondition {}

    /** Run only when the resource's etag over the scope still equals this one. */
    record IfMatch(String etag) implements Precondition {
        public IfMatch {
            Objects.requireNonNull(etag, "etag");
        }
    }

    static Precondition none() {
        return new None();
    }

    static Precondition ifMatch(String etag) {
        return new IfMatch(etag);
    }
}
