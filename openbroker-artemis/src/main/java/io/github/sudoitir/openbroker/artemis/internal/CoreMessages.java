package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.jspecify.annotations.Nullable;

/**
 * Messages over Core: what the broker holds, read and written without management's limits. Bodies
 * are whole and properties keep their types. Never deserializes an object message.
 */
final class CoreMessages {

    /** The JMS-visible names of the headers the broker keeps as properties. */
    private static final String GROUP_ID = "JMSXGroupID";

    private static final String DELIVERY_COUNT = "JMSXDeliveryCount";

    private CoreMessages() {}

    /** Sends one message to a JMS destination name: an address, or {@code address::queue}. */
    static String send(Session session, String destination, OutboundMessage out, Clock clock) throws JMSException {
        MessageHeaders h = out.headers();
        if (h.id().isPresent() || h.key().isPresent()) {
            throw Queries.invalid("An Artemis message gets its id from the broker and takes no key.");
        }
        byte[] body = out.body().bytes().map(Blob::toByteArray).orElse(null);
        jakarta.jms.Message message;
        if (body == null) {
            BytesMessage bytes = session.createBytesMessage();
            try (var in = out.body().openStream()) {
                byte[] chunk = new byte[64 * 1024];
                for (int n; (n = in.read(chunk)) > 0; ) {
                    bytes.writeBytes(chunk, 0, n);
                }
            } catch (IOException e) {
                throw Queries.invalid("The message body could not be read: " + e.getMessage());
            }
            message = bytes;
        } else if (isText(h, body)) {
            message = session.createTextMessage(new String(body, StandardCharsets.UTF_8));
        } else {
            BytesMessage bytes = session.createBytesMessage();
            bytes.writeBytes(body);
            message = bytes;
        }
        for (Map.Entry<String, PropertyValue> p : out.properties().entrySet()) {
            if (p.getValue() instanceof PropertyValue.OfBytes) {
                throw Queries.invalid("A JMS property cannot hold bytes: " + p.getKey() + ".");
            }
            message.setObjectProperty(p.getKey(), p.getValue().boxed());
        }
        if (h.correlationId().isPresent()) {
            message.setJMSCorrelationID(h.correlationId().get());
        }
        if (h.type().isPresent()) {
            message.setJMSType(h.type().get());
        }
        if (h.replyTo().isPresent()) {
            message.setJMSReplyTo(session.createQueue(h.replyTo().get()));
        }
        if (h.groupId().isPresent()) {
            message.setStringProperty(GROUP_ID, h.groupId().get());
        }
        if (h.deduplicationId().isPresent()) {
            message.setStringProperty(
                    BrowseRows.DUPLICATE_ID, h.deduplicationId().get());
        }
        if (h.deliverAt().isPresent()) {
            message.setLongProperty(
                    BrowseRows.SCHEDULED_DELIVERY, h.deliverAt().get().toEpochMilli());
        }
        int mode = h.durable().orElse(true) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        int priority = h.priority().isPresent() ? h.priority().getAsInt() : jakarta.jms.Message.DEFAULT_PRIORITY;
        long ttl = h.expiresAt()
                .map(at -> Math.max(1L, at.toEpochMilli() - clock.millis()))
                .orElse(0L);
        Queue target = session.createQueue(destination);
        try (MessageProducer producer = session.createProducer(target)) {
            producer.send(message, mode, priority, ttl);
        }
        return message.getJMSMessageID();
    }

    /** A body is sent as text when it is UTF-8 and nothing says it is binary. */
    private static boolean isText(MessageHeaders h, byte[] body) {
        boolean textual = h.contentType()
                .map(t -> t.startsWith("text/") || t.contains("json") || t.contains("xml"))
                .orElse(true);
        if (!textual) {
            return false;
        }
        try {
            StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(body));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /** One received or browsed JMS message, as the SDK's message. */
    static Message read(jakarta.jms.Message m, ResourceId subscription, NodeId node, long maxBodyBytes)
            throws JMSException {
        Set<MessagePart> truncated = EnumSet.noneOf(MessagePart.class);
        byte[] body;
        int type;
        switch (m) {
            case TextMessage text -> {
                String t = text.getText();
                body = t == null ? new byte[0] : t.getBytes(StandardCharsets.UTF_8);
                type = 3;
            }
            case BytesMessage bytes -> {
                bytes.reset();
                long length = bytes.getBodyLength();
                body = new byte[(int) Math.min(Math.min(length, maxBodyBytes), Integer.MAX_VALUE - 8)];
                bytes.readBytes(body);
                if (length > body.length) {
                    truncated.add(MessagePart.BODY);
                }
                type = 4;
            }
            default -> {
                // Object, map and stream bodies are not read: an object body would need deserializing.
                body = new byte[0];
                type = 0;
            }
        }
        if (body.length > maxBodyBytes) {
            body = Arrays.copyOf(body, (int) maxBodyBytes);
            truncated.add(MessagePart.BODY);
        }

        Map<String, PropertyValue> properties = new LinkedHashMap<>();
        Enumeration<?> names = m.getPropertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            PropertyValue value = typed(m.getObjectProperty(name));
            if (value != null) {
                properties.put(name, value);
            }
        }
        String group = text(properties.remove(GROUP_ID));
        String coreGroup = text(properties.remove(BrowseRows.GROUP_ID));
        String duplicate = text(properties.remove(BrowseRows.DUPLICATE_ID));
        PropertyValue scheduled = properties.remove(BrowseRows.SCHEDULED_DELIVERY);
        PropertyValue deliveries = properties.remove(DELIVERY_COUNT);

        MessageHeaders.Builder headers = MessageHeaders.builder()
                .id(m.getJMSMessageID())
                .correlationId(m.getJMSCorrelationID())
                .type(m.getJMSType())
                .replyTo(destinationName(m.getJMSReplyTo()))
                .groupId(group != null ? group : coreGroup)
                .deduplicationId(duplicate)
                .priority(m.getJMSPriority())
                .durable(m.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
        if (m.getJMSTimestamp() > 0) {
            headers.producedAt(Instant.ofEpochMilli(m.getJMSTimestamp()));
        }
        if (m.getJMSExpiration() > 0) {
            headers.expiresAt(Instant.ofEpochMilli(m.getJMSExpiration()));
        }
        if (scheduled instanceof PropertyValue.OfLong(long at)) {
            headers.deliverAt(Instant.ofEpochMilli(at));
        }
        if (deliveries instanceof PropertyValue.OfInt(int count)) {
            headers.deliveryCount(count);
        }
        long id = 0L;
        if (m instanceof ActiveMQMessage artemis) {
            id = artemis.getCoreMessage().getMessageID();
        }
        return new Message(
                new MessageRef(subscription, Shard.node(node), String.valueOf(id)),
                headers.build(),
                properties,
                MessageBody.of(Blob.of(body)),
                truncated,
                Map.of(
                        "artemis",
                        Attributes.builder()
                                .putRaw(BrowseRows.MESSAGE_TYPE, type)
                                .build()));
    }

    private static @Nullable PropertyValue typed(@Nullable Object v) {
        return switch (v) {
            case null -> null;
            case Boolean b -> PropertyValue.of(b);
            case Byte b -> new PropertyValue.OfByte(b);
            case Short s -> new PropertyValue.OfShort(s);
            case Integer i -> PropertyValue.of(i);
            case Long l -> PropertyValue.of(l);
            case Float f -> new PropertyValue.OfFloat(f);
            case Double d -> PropertyValue.of(d);
            case byte[] bytes -> new PropertyValue.OfBytes(Blob.of(bytes));
            default -> PropertyValue.of(v.toString());
        };
    }

    private static @Nullable String text(@Nullable PropertyValue value) {
        return value == null ? null : value.asText();
    }

    private static @Nullable String destinationName(@Nullable Destination destination) throws JMSException {
        return switch (destination) {
            case null -> null;
            case Queue q -> q.getQueueName();
            case Topic t -> t.getTopicName();
            default -> destination.toString();
        };
    }
}
