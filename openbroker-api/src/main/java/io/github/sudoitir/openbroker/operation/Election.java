package io.github.sudoitir.openbroker.operation;

/** Which replica may become leader. */
public enum Election {
    /** The preferred replica, when it is in sync. */
    PREFERRED,
    /** Any replica, even one not in sync: may lose data. */
    UNCLEAN
}
