package io.github.sudoitir.openbroker;

/**
 * The canonical status of a failure: the gRPC codes, which every client already knows how to
 * handle. {@link ErrorInfo#reason()} says precisely what happened within the status.
 */
public enum Status {
    /** The caller cancelled the call; a driver reports interruption this way. */
    CANCELLED,
    /** The failure could not be classified. */
    UNKNOWN,
    /** The request is wrong whatever the state of the system: a bad name, an invalid filter. */
    INVALID_ARGUMENT,
    /** The call's deadline passed before it finished. */
    DEADLINE_EXCEEDED,
    /** The resource does not exist. */
    NOT_FOUND,
    /** The resource already exists, differently. */
    ALREADY_EXISTS,
    /** The account is known but may not do this. */
    PERMISSION_DENIED,
    /** A limit was reached: a rate, a quota, a full destination, a consumer limit. */
    RESOURCE_EXHAUSTED,
    /** The system is not in the state the call needs: a precondition failed, consumers are attached. */
    FAILED_PRECONDITION,
    /** The call conflicted with a concurrent one and was aborted. */
    ABORTED,
    /** A position past the valid range: an offset beyond retention, a stale cursor. */
    OUT_OF_RANGE,
    /** The system or the driver cannot do this. */
    UNIMPLEMENTED,
    /** The system broke an invariant: a malformed answer, an internal error. */
    INTERNAL,
    /** The system cannot be reached right now; a retry may succeed. */
    UNAVAILABLE,
    /** Data was lost or corrupted. */
    DATA_LOSS,
    /** The account was not accepted. */
    UNAUTHENTICATED
}
