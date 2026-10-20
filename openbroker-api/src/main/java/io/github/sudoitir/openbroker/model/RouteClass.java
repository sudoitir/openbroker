package io.github.sudoitir.openbroker.model;

/** What a route does. */
public enum RouteClass {
    /** Connects a destination to a subscription or another destination inside one system. */
    BINDING,
    /** Copies or diverts messages to another destination. */
    FORWARD,
    /** Moves messages to another node or system: a bridge, a shovel, a federation link. */
    LINK
}
