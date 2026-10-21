package io.github.sudoitir.openbroker.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;

/** The attribute names the observer writes. The semantic conventions for messaging are still in development, so they are defined here. */
final class MessagingAttributes {

    static final AttributeKey<String> SYSTEM = AttributeKey.stringKey("messaging.system");
    static final AttributeKey<String> OPERATION_TYPE = AttributeKey.stringKey("messaging.operation.type");
    static final AttributeKey<String> OPERATION_NAME = AttributeKey.stringKey("messaging.operation.name");
    static final AttributeKey<String> DESTINATION_NAME = AttributeKey.stringKey("messaging.destination.name");
    static final AttributeKey<String> SUBSCRIPTION_NAME =
            AttributeKey.stringKey("messaging.destination.subscription.name");
    static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("messaging.message.id");
    static final AttributeKey<String> CONVERSATION_ID = AttributeKey.stringKey("messaging.message.conversation_id");
    static final AttributeKey<Long> BODY_SIZE = AttributeKey.longKey("messaging.message.body.size");
    static final AttributeKey<Long> BATCH_COUNT = AttributeKey.longKey("messaging.batch.message_count");
    static final AttributeKey<String> SERVER_ADDRESS = AttributeKey.stringKey("server.address");
    static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");
    static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
    static final AttributeKey<String> LINK_STATE = AttributeKey.stringKey("openbroker.link.state");

    static final String OPERATION_DURATION = "messaging.client.operation.duration";
    static final String SENT_MESSAGES = "messaging.client.sent.messages";
    static final String CONSUMED_MESSAGES = "messaging.client.consumed.messages";
    static final String PROCESS_DURATION = "messaging.process.duration";
    static final String LINK_RECOVERIES = "openbroker.messaging.link.recoveries";

    private MessagingAttributes() {}
}
