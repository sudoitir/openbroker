package io.github.sudoitir.openbroker;

/**
 * Which side of a broker a call or endpoint belongs to. Systems often expose them on different
 * ports, protocols and accounts.
 */
public enum Plane {
    /** Administration and observation: reading and changing resources. */
    MANAGEMENT,
    /** Messages: sending, receiving, browsing and notifications. */
    DATA
}
