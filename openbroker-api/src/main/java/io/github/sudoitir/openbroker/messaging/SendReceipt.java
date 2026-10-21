package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.NodeId;
import java.util.Objects;
import java.util.Optional;

/**
 * What the system said about a sent message.
 *
 * @param messageId the id the system gave it
 * @param ref where it can be found again
 * @param node the node that took it
 * @param resent the message was sent again after the link recovered
 */
public record SendReceipt(Optional<String> messageId, Optional<MessageRef> ref, Optional<NodeId> node, boolean resent) {

    public SendReceipt {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(node, "node");
    }

    /** A receipt that says only that the message was accepted. */
    public static SendReceipt accepted() {
        return new SendReceipt(Optional.empty(), Optional.empty(), Optional.empty(), false);
    }
}
