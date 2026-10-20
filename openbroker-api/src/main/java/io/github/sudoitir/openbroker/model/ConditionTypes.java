package io.github.sudoitir.openbroker.model;

/** Standard condition types. */
public final class ConditionTypes {

    /** The resource accepts no more messages: a full destination, a blocked address. */
    public static final String FULL = "openbroker.full";
    /** Delivery or publishing is paused. */
    public static final String PAUSED = "openbroker.paused";
    /** Fewer replicas than configured are in sync. */
    public static final String UNDER_REPLICATED = "openbroker.under_replicated";
    /** A node's replica has fallen behind. */
    public static final String OUT_OF_SYNC = "openbroker.out_of_sync";
    /** The node or resource is serving. */
    public static final String SERVING = "openbroker.serving";

    private ConditionTypes() {}
}
