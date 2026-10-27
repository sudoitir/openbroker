package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.BulkOutcome;
import io.github.sudoitir.openbroker.messaging.MessageAction;
import io.github.sudoitir.openbroker.messaging.MessageField;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.Map;
import java.util.Optional;

/** Browsing, counting and sending messages. */
public final class KafkaMessages implements Messages {

    private final KafkaCluster cluster;

    public KafkaMessages(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request) {
        throw KafkaErrors.unimplemented("browse messages yet");
    }

    @Override
    public Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter) {
        throw KafkaErrors.unimplemented("count messages yet");
    }

    @Override
    public Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field) {
        throw KafkaErrors.unimplemented("count messages yet");
    }

    @Override
    public SendReceipt send(SendTarget target, Scope scope, OutboundMessage message) {
        throw KafkaErrors.unimplemented("send messages yet");
    }

    @Override
    public Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action) {
        throw KafkaErrors.unimplemented("act on messages yet");
    }
}
