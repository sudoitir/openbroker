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

    /** Sends one message. */
    SendReceipt send(SendTarget target, OutboundMessage message);

    /** Applies an action to the selected messages. */
    Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action);
}
