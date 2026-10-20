package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.Map;
import java.util.Optional;

/** Reading and acting on the messages a subscription holds. */
public interface Messages {

    /** Reads messages without consuming them. */
    BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request);

    /** How many messages match, and whether the count is exact. */
    Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter);

    /** The number of messages per value of a field, for triage. */
    Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field);

    /**
     * Sends one message through the nodes of {@code scope}: the first that holds the target takes it.
     * Where the system routes between nodes itself, the scope only says where to enter.
     */
    SendReceipt send(SendTarget target, Scope scope, OutboundMessage message);

    /** Sends one message through any node. */
    default SendReceipt send(SendTarget target, OutboundMessage message) {
        return send(target, Scope.whole(), message);
    }

    /** Applies an action to the selected messages. */
    Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action);
}
