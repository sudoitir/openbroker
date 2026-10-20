package io.github.sudoitir.openbroker;

/** Whether a failed request reached the system, which decides whether a retry is safe. */
public enum Dispatch {
    /** The request never left: a retry cannot duplicate its effect. */
    NOT_SENT,
    /** The request may or may not have taken effect: retry only what is idempotent. */
    IN_DOUBT,
    /** The system received the request and refused it: it took no effect. */
    REJECTED
}
