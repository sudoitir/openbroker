package io.github.sudoitir.openbroker.kafka.codec;

import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaErrors;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * Turns a message into a Kafka record and back. A record has a key, a value and headers of bytes, so
 * the standard fields of a message travel as headers named under {@value #PREFIX}, and a message's
 * own properties as headers of their own name. A property that is not a string says its type in a
 * header {@value #TYPE_PREFIX}&lt;name&gt;, so a number comes back a number; one that does not (a record another
 * application wrote) comes back a string, or bytes if it is not text. Every reader and writer of this
 * driver uses this codec, so a message sent by one comes out whole at another.
 */
public final class RecordCodec {

    /** The prefix of every header this codec writes for the message's standard fields. */
    public static final String PREFIX = "openbroker.";

    public static final String ID = PREFIX + "id";
    public static final String CORRELATION_ID = PREFIX + "correlation-id";
    public static final String REPLY_TO = PREFIX + "reply-to";
    public static final String CONTENT_TYPE = PREFIX + "content-type";
    public static final String TYPE = PREFIX + "type";
    public static final String GROUP_ID = PREFIX + "group-id";
    public static final String DEDUPLICATION_ID = PREFIX + "deduplication-id";
    public static final String ORIGIN = PREFIX + "origin";
    public static final String ORIGIN_ID = PREFIX + "origin-id";

    /** The prefix of the header that names the type of the property after it. */
    public static final String TYPE_PREFIX = PREFIX + "type.";

    /** The namespace of the native data on a message. */
    public static final String EXTENSION = "kafka";

    private RecordCodec() {}

    // Writing.

    /**
     * The record for a message. The key is the message's key, else its group id, else the routing key
     * given; a key keeps messages that share it in order on one partition.
     */
    public static ProducerRecord<byte[], byte[]> record(
            String topic, Optional<Integer> partition, Optional<String> routingKey, OutboundMessage message) {
        MessageHeaders h = message.headers();
        refuse(h.priority().isPresent(), "a priority");
        refuse(h.expiresAt().isPresent(), "an expiry");
        refuse(h.deliverAt().isPresent(), "a delivery time");
        byte[] key = h.key()
                .map(Blob::toByteArray)
                .or(() -> h.groupId().map(g -> g.getBytes(StandardCharsets.UTF_8)))
                .or(() -> routingKey.map(k -> k.getBytes(StandardCharsets.UTF_8)))
                .orElse(null);
        Headers headers = new RecordHeaders();
        text(headers, ID, h.id());
        text(headers, CORRELATION_ID, h.correlationId());
        text(headers, REPLY_TO, h.replyTo());
        text(headers, CONTENT_TYPE, h.contentType());
        text(headers, TYPE, h.type());
        // The group id is the key when there is no other; it is kept as a header when a key is given too.
        if (h.key().isPresent()) {
            text(headers, GROUP_ID, h.groupId());
        }
        text(headers, DEDUPLICATION_ID, h.deduplicationId());
        text(headers, ORIGIN, h.origin());
        text(headers, ORIGIN_ID, h.originId());
        message.properties().forEach((name, value) -> property(headers, name, value));
        return new ProducerRecord<>(
                topic,
                partition.orElse(null),
                h.producedAt().map(Instant::toEpochMilli).orElse(null),
                key,
                value(message.body()),
                headers);
    }

    private static void refuse(boolean present, String what) {
        if (present) {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "A Kafka record has no place for " + what + "; leave it out.");
        }
    }

    private static void text(Headers headers, String name, Optional<String> value) {
        value.ifPresent(v -> headers.add(new RecordHeader(name, v.getBytes(StandardCharsets.UTF_8))));
    }

    private static void property(Headers headers, String name, PropertyValue value) {
        if (name.isEmpty() || name.startsWith(PREFIX)) {
            throw KafkaErrors.refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    "A property cannot be named \"" + name + "\": names starting with " + PREFIX + " are reserved.");
        }
        switch (value) {
            case PropertyValue.OfString s -> headers.add(new RecordHeader(name, utf8(s.value())));
            case PropertyValue.OfBytes b -> {
                headers.add(new RecordHeader(name, b.value().toByteArray()));
                headers.add(new RecordHeader(TYPE_PREFIX + name, utf8("bytes")));
            }
            default -> {
                headers.add(new RecordHeader(name, utf8(value.asText())));
                headers.add(new RecordHeader(TYPE_PREFIX + name, utf8(typeName(value))));
            }
        }
    }

    private static String typeName(PropertyValue value) {
        return switch (value) {
            case PropertyValue.OfBoolean v -> "boolean";
            case PropertyValue.OfByte v -> "byte";
            case PropertyValue.OfShort v -> "short";
            case PropertyValue.OfInt v -> "int";
            case PropertyValue.OfLong v -> "long";
            case PropertyValue.OfFloat v -> "float";
            case PropertyValue.OfDouble v -> "double";
            case PropertyValue.OfString v -> "string";
            case PropertyValue.OfBytes v -> "bytes";
        };
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

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    // Reading.

    /** The message in a record read from {@code source}, its body cut to {@code maxBodyBytes}. */
    public static Message message(ResourceId source, ConsumerRecord<byte[], byte[]> record, long maxBodyBytes) {
        Map<String, byte[]> raw = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            raw.put(header.key(), header.value() == null ? new byte[0] : header.value());
        }
        MessageHeaders.Builder headers = MessageHeaders.builder();
        headers.id(string(raw, ID));
        headers.correlationId(string(raw, CORRELATION_ID));
        headers.replyTo(string(raw, REPLY_TO));
        headers.contentType(string(raw, CONTENT_TYPE));
        headers.type(string(raw, TYPE));
        headers.groupId(string(raw, GROUP_ID));
        headers.deduplicationId(string(raw, DEDUPLICATION_ID));
        headers.origin(string(raw, ORIGIN));
        headers.originId(string(raw, ORIGIN_ID));
        if (record.timestamp() >= 0) {
            headers.producedAt(Instant.ofEpochMilli(record.timestamp()));
        }
        if (record.key() != null) {
            headers.key(Blob.of(record.key()));
        }
        Map<String, PropertyValue> properties = new LinkedHashMap<>();
        raw.forEach((name, bytes) -> {
            if (!name.startsWith(PREFIX)) {
                properties.put(name, property(bytes, string(raw, TYPE_PREFIX + name)));
            }
        });

        Set<MessagePart> truncated = new HashSet<>();
        MessageBody body;
        byte[] value = record.value();
        if (value == null) {
            body = MessageBody.tombstone();
        } else if (value.length > maxBodyBytes) {
            truncated.add(MessagePart.BODY);
            body = MessageBody.of(Arrays.copyOf(value, (int) maxBodyBytes));
        } else {
            body = MessageBody.of(value);
        }
        Attributes.Builder native_ = Attributes.builder()
                .putRaw("topic", record.topic())
                .putRaw("partition", (long) record.partition())
                .putRaw("offset", record.offset())
                .putRaw("timestamp_type", record.timestampType().name().toLowerCase(Locale.ROOT));
        record.leaderEpoch().ifPresent(e -> native_.putRaw("leader_epoch", (long) e));
        return new Message(
                new MessageRef(
                        source,
                        Shard.partition(PartitionId.of(String.valueOf(record.partition()))),
                        String.valueOf(record.offset())),
                headers.build(),
                properties,
                body,
                truncated,
                Map.of(EXTENSION, native_.build()));
    }

    private static @Nullable String string(Map<String, byte[]> raw, String name) {
        byte[] bytes = raw.get(name);
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static PropertyValue property(byte[] bytes, @Nullable String type) {
        if (type == null) {
            return text(bytes)
                    .<PropertyValue>map(PropertyValue::of)
                    .orElseGet(() -> new PropertyValue.OfBytes(Blob.of(bytes)));
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        try {
            return switch (type) {
                case "bytes" -> new PropertyValue.OfBytes(Blob.of(bytes));
                case "boolean" -> new PropertyValue.OfBoolean(Boolean.parseBoolean(text));
                case "byte" -> new PropertyValue.OfByte(Byte.parseByte(text));
                case "short" -> new PropertyValue.OfShort(Short.parseShort(text));
                case "int" -> new PropertyValue.OfInt(Integer.parseInt(text));
                case "long" -> new PropertyValue.OfLong(Long.parseLong(text));
                case "float" -> new PropertyValue.OfFloat(Float.parseFloat(text));
                case "double" -> new PropertyValue.OfDouble(Double.parseDouble(text));
                default -> PropertyValue.of(text);
            };
        } catch (NumberFormatException e) {
            // The type header does not describe the value: it is shown as written.
            return PropertyValue.of(text);
        }
    }

    private static Optional<String> text(byte[] bytes) {
        try {
            return Optional.of(StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString());
        } catch (CharacterCodingException e) {
            return Optional.empty();
        }
    }
}
