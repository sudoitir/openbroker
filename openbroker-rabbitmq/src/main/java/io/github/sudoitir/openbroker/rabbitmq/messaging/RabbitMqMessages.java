package io.github.sudoitir.openbroker.rabbitmq.messaging;

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
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.util.Map;
import java.util.Optional;

/**
 * {@link Messages}: browse (streams only), count, send, and the {@code UNIMPLEMENTED} count by a field and bulk
 * actions.
 */
public final class RabbitMqMessages implements Messages, AutoCloseable {

    public RabbitMqMessages(RabbitMqCluster cluster) {}

    @Override
    public BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request) {
        throw RabbitMqErrors.unimplemented("RabbitMqMessages.browse");
    }

    @Override
    public Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter) {
        throw RabbitMqErrors.unimplemented("RabbitMqMessages.count");
    }

    @Override
    public Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field) {
        throw RabbitMqErrors.unimplemented("RabbitMqMessages.countBy");
    }

    @Override
    public SendReceipt send(SendTarget target, Scope scope, OutboundMessage message) {
        throw RabbitMqErrors.unimplemented("RabbitMqMessages.send");
    }

    @Override
    public Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action) {
        throw RabbitMqErrors.unimplemented("RabbitMqMessages.bulk");
    }

    @Override
    public void close() {}
}
