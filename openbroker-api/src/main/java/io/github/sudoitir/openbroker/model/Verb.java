package io.github.sudoitir.openbroker.model;

/**
 * What may be done to a kind, declared statically by the driver, like Kubernetes API verbs. A verb
 * missing from a kind means the system cannot do it; {@code Access} says whether this account may.
 */
public enum Verb {
    LIST,
    GET,
    CREATE,
    UPDATE,
    DELETE,
    PAUSE,
    RESUME,
    PURGE,
    SEEK,
    TRUNCATE,
    CLOSE,
    REASSIGN,
    ELECT_LEADER,
    RESOLVE,
    BROWSE,
    COUNT,
    SEND,
    RECEIVE,
    MOVE_MESSAGES,
    COPY_MESSAGES,
    DELETE_MESSAGES,
    RETRY_MESSAGES,
    DEAD_LETTER_MESSAGES
}
