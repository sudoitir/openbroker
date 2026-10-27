package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * The mapping between the SDK's messages and Kafka's records. The standard headers and the typed
 * properties travel as record headers named {@code openbroker.*}; the record key is the message
 * key, or the group id when there is none, so a group stays on one partition.
 */
final class Records {

    /** The extension a received message carries: its topic, partition and offset. */
    static final String EXTENSION = "kafka";

    static final String ORIGIN_TOPIC = "openbroker.origin.topic";
    static final String ORIGIN_PARTITION = "openbroker.origin.partition";
    static final String ORIGIN_OFFSET = "openbroker.origin.offset";
    static final String ORIGIN_ATTEMPTS = "openbroker.origin.attempts";
    static final String ORIGIN_REASON = "openbroker.origin.reason";
    static final String ATTEMPT = "openbroker.retry.attempt";
    static final String DUE = "openbroker.retry.due";

    private static final String ID = "openbroker.id";
    private static final String CORRELATION_ID = "openbroker.correlation_id";
    private static final String REPLY_TO = "openbroker.reply_to";
    private static final String CONTENT_TYPE = "openbroker.content_type";
    private static final String TYPE = "openbroker.type";
    private static final String PRIORITY = "openbroker.priority";
    private static final String EXPIRES_AT = "openbroker.expires_at";
    private static final String GROUP_ID = "openbroker.group_id";
    private static final String DEDUPLICATION_ID = "openbroker.deduplication_id";
    private static final String ORIGIN = "openbroker.origin";
    private static final String ORIGIN_ID = "openbroker.origin_id";
    private static final String PROPERTY = "openbroker.prop.";
    static final String ATTEMPTS_PROPERTY = "openbroker.dead_letter_attempts";
    static final String REASON_PROPERTY = "openbroker.dead_letter_reason";

    private static final Set<String> RESERVED_PROPERTIES = Set.of(ATTEMPTS_PROPERTY, REASON_PROPERTY);

    private Records() {}

    // ---- sending --------------------------------------------------------------------------

    /**
     * The record for a message to {@code topic}.
     *
     * @param partition the partition to send to, or null to let the key decide
     * @param routingKey the key when the message names none
     */
    static ProducerRecord<byte[], byte[]> record(
            String topic, @Nullable Integer partition, Optional<String> routingKey, OutboundMessage message) {
        MessageHeaders h = message.headers();
        byte[] key = h.key().map(Blob::toByteArray).orElse(null);
        if (key == null) {
            String fallback = routingKey.or(h::groupId).orElse(null);
            key = fallback == null ? null : fallback.getBytes(StandardCharsets.UTF_8);
        }
        Headers headers = new RecordHeaders();
        text(headers, ID, h.id());
        text(headers, CORRELATION_ID, h.correlationId());
        text(headers, REPLY_TO, h.replyTo());
        text(headers, CONTENT_TYPE, h.contentType());
        text(headers, TYPE, h.type());
        h.priority().ifPresent(p -> text(headers, PRIORITY, Optional.of(Integer.toString(p))));
        text(headers, EXPIRES_AT, h.expiresAt().map(Instant::toString));
        text(headers, GROUP_ID, h.groupId());
        text(headers, DEDUPLICATION_ID, h.deduplicationId());
        text(headers, ORIGIN, h.origin());
        text(headers, ORIGIN_ID, h.originId());
        for (Map.Entry<String, PropertyValue> property : message.properties().entrySet()) {
            if (!RESERVED_PROPERTIES.contains(property.getKey())) {
                headers.add(PROPERTY + property.getKey(), encode(property.getValue()));
            }
        }
        deadLetterFacts(message).forEach((name, value) -> headers.add(name, value.getBytes(StandardCharsets.UTF_8)));
        return new ProducerRecord<>(topic, partition, null, key, value(message.body()), headers);
    }

    /**
     * The headers of a dead letter: its origin, the attempts and the reason. A message is a dead
     * letter when it carries the properties the engine puts on the copy it dead-letters.
     */
    private static Map<String, String> deadLetterFacts(OutboundMessage message) {
        Map<String, String> facts = new LinkedHashMap<>();
        PropertyValue attempts = message.properties().get(ATTEMPTS_PROPERTY);
        PropertyValue reason = message.properties().get(REASON_PROPERTY);
        if (attempts == null && reason == null) {
            return facts;
        }
        if (attempts != null) {
            facts.put(ORIGIN_ATTEMPTS, attempts.asText());
        }
        if (reason != null) {
            facts.put(ORIGIN_REASON, reason.asText());
        }
        Attributes kafka = message.extensions().getOrDefault(EXTENSION, Attributes.empty());
        for (String name : new String[] {"topic", "partition", "offset"}) {
            kafka.raw("origin_" + name)
                    .or(() -> kafka.raw(name))
                    .ifPresent(v -> facts.put("openbroker.origin." + name, String.valueOf(v)));
        }
        return facts;
    }

    private static byte @Nullable [] value(MessageBody body) {
        if (body.isTombstone()) {
            return null;
        }
        if (body.bytes().isPresent()) {
            return body.bytes().get().toByteArray();
        }
        try (InputStream in = body.openStream()) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void text(Headers headers, String name, Optional<String> value) {
        value.ifPresent(v -> headers.add(name, v.getBytes(StandardCharsets.UTF_8)));
    }

    // ---- receiving ------------------------------------------------------------------------

    /**
     * The message for a record.
     *
     * @param source the subscription, or the topic when read without a group
     * @param deliveryCount the 1-based delivery attempt
     */
    static Message message(ResourceId source, ConsumerRecord<byte[], byte[]> record, int deliveryCount) {
        MessageHeaders.Builder h = MessageHeaders.builder()
                .key(record.key() == null ? null : Blob.of(record.key()))
                .producedAt(Instant.ofEpochMilli(record.timestamp()))
                .deliveryCount(deliveryCount)
                .durable(true);
        Map<String, PropertyValue> properties = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            String name = header.key();
            byte[] raw = header.value() == null ? new byte[0] : header.value();
            if (name.startsWith(PROPERTY)) {
                properties.put(name.substring(PROPERTY.length()), decode(raw));
                continue;
            }
            switch (name) {
                case ID -> h.id(utf8(raw));
                case CORRELATION_ID -> h.correlationId(utf8(raw));
                case REPLY_TO -> h.replyTo(utf8(raw));
                case CONTENT_TYPE -> h.contentType(utf8(raw));
                case TYPE -> h.type(utf8(raw));
                case PRIORITY -> h.priority(Integer.valueOf(utf8(raw)));
                case EXPIRES_AT -> h.expiresAt(Instant.parse(utf8(raw)));
                case GROUP_ID -> h.groupId(utf8(raw));
                case DEDUPLICATION_ID -> h.deduplicationId(utf8(raw));
                case ORIGIN -> h.origin(utf8(raw));
                case ORIGIN_ID -> h.originId(utf8(raw));
                case ORIGIN_TOPIC, ORIGIN_PARTITION, ORIGIN_OFFSET, ORIGIN_ATTEMPTS, ORIGIN_REASON, ATTEMPT, DUE -> {}
                default -> properties.put(name, new PropertyValue.OfBytes(Blob.of(raw)));
            }
        }
        MessageBody body = record.value() == null ? MessageBody.tombstone() : MessageBody.of(record.value());
        Attributes.Builder kafka = Attributes.builder()
                .putRaw("topic", record.topic())
                .putRaw("partition", record.partition())
                .putRaw("offset", record.offset());
        // A record that went through a retry topic, or was dead-lettered, says where it was first read.
        for (String name : new String[] {"topic", "partition", "offset", "attempts", "reason"}) {
            header(record, "openbroker.origin." + name).ifPresent(value -> kafka.putRaw("origin_" + name, value));
        }
        return new Message(
                new MessageRef(
                        source,
                        Shard.partition(PartitionId.of(Integer.toString(record.partition()))),
                        Long.toString(record.offset())),
                h.build(),
                properties,
                body,
                Set.of(),
                Map.of(EXTENSION, kafka.build()));
    }

    /** The value of a record header, as text; empty when there is none. */
    static Optional<String> header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? Optional.empty() : Optional.of(utf8(header.value()));
    }

    private static String utf8(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    // ---- typed properties: a one-letter type, then the value ------------------------------

    private static byte[] encode(PropertyValue value) {
        return switch (value) {
            case PropertyValue.OfBoolean v -> tagged('Z', Boolean.toString(v.value()));
            case PropertyValue.OfByte v -> tagged('B', Byte.toString(v.value()));
            case PropertyValue.OfShort v -> tagged('S', Short.toString(v.value()));
            case PropertyValue.OfInt v -> tagged('I', Integer.toString(v.value()));
            case PropertyValue.OfLong v -> tagged('J', Long.toString(v.value()));
            case PropertyValue.OfFloat v -> tagged('F', Float.toString(v.value()));
            case PropertyValue.OfDouble v -> tagged('D', Double.toString(v.value()));
            case PropertyValue.OfString v -> tagged('T', v.value());
            case PropertyValue.OfBytes v -> {
                byte[] bytes = v.value().toByteArray();
                ByteBuffer out = ByteBuffer.allocate(bytes.length + 1);
                out.put((byte) 'X').put(bytes);
                yield out.array();
            }
        };
    }

    private static byte[] tagged(char tag, String text) {
        return (tag + text).getBytes(StandardCharsets.UTF_8);
    }

    private static PropertyValue decode(byte[] raw) {
        if (raw.length == 0) {
            return new PropertyValue.OfString("");
        }
        char tag = (char) raw[0];
        if (tag == 'X') {
            return new PropertyValue.OfBytes(Blob.of(Arrays.copyOfRange(raw, 1, raw.length)));
        }
        String text = new String(raw, 1, raw.length - 1, StandardCharsets.UTF_8);
        return switch (tag) {
            case 'Z' -> new PropertyValue.OfBoolean(Boolean.parseBoolean(text));
            case 'B' -> new PropertyValue.OfByte(Byte.parseByte(text));
            case 'S' -> new PropertyValue.OfShort(Short.parseShort(text));
            case 'I' -> new PropertyValue.OfInt(Integer.parseInt(text));
            case 'J' -> new PropertyValue.OfLong(Long.parseLong(text));
            case 'F' -> new PropertyValue.OfFloat(Float.parseFloat(text));
            case 'D' -> new PropertyValue.OfDouble(Double.parseDouble(text));
            default -> new PropertyValue.OfString(text);
        };
    }
}
