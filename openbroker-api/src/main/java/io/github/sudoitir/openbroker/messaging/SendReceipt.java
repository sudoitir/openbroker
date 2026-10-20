package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.NodeId;
import java.util.Objects;
import java.util.Optional;

/** What the system said about a sent message. */
public record SendReceipt(Optional<String> messageId, Optional<MessageRef> ref, Optional<NodeId> node) {

    public SendReceipt {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(node, "node");
    }

    public static SendReceipt accepted() {
        return new SendReceipt(Optional.empty(), Optional.empty(), Optional.empty());
    }
}
