package io.github.sudoitir.openbroker.messaging;

/** What a release on a pull receiver covers. */
public enum SettlementScope {
    /** The one message settled. */
    MESSAGE,
    /** Every unsettled message of the receiver. */
    RECEIVER
}
