package io.github.sudoitir.openbroker;

/** What a call did on one node. */
public enum NodeOutcome {
    /** The call took effect there. */
    APPLIED,
    /** The call failed there and took no effect. */
    FAILED,
    /** The call may or may not have taken effect there. */
    IN_DOUBT,
    /** The call was not attempted there. */
    SKIPPED
}
