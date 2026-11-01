package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;

/** Maps AMQP 0-9-1 properties and bodies to and from the SDK {@link Message} and {@link OutboundMessage}. */
public final class MessageCodec {

    private MessageCodec() {}

    public static AMQP.BasicProperties properties(OutboundMessage message) {
        throw RabbitMqErrors.unimplemented("MessageCodec.properties");
    }

    public static byte[] body(OutboundMessage message) {
        throw RabbitMqErrors.unimplemented("MessageCodec.body");
    }

    public static Message message(String queue, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        throw RabbitMqErrors.unimplemented("MessageCodec.message");
    }
}
