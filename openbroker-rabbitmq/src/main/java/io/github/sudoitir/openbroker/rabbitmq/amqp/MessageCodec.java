package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
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
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Maps AMQP 0-9-1 properties and bodies to and from the SDK {@link Message} and {@link OutboundMessage}.
 *
 * <p>The standard properties map to the same-named header. An application header with a scalar value is an SDK
 * property of the matching type; a header that holds a timestamp, a decimal, a table or an array, such as
 * {@code x-death}, is kept in the {@code rabbitmq} extension as {@code header.<name>} in plain Java types and is
 * never deserialised. The extension also carries what AMQP has no header for: {@code exchange},
 * {@code routing_key}, {@code redelivered}, {@code x-stream-offset}, {@code content-encoding}, {@code expiration},
 * {@code user-id} and {@code app-id}. The delivery tag is the receiver's business and is not in the message.
 */
public final class MessageCodec {

    /** The namespace of the extension this codec reads and writes. */
    public static final String EXTENSION = "rabbitmq";

    /** The extension attribute of the exchange a message was published to. */
    public static final String EXCHANGE = "exchange";

    /** The extension attribute of the routing key a message was published with. */
    public static final String ROUTING_KEY = "routing_key";

    /** The extension attribute that is true when the broker delivers the message again. */
    public static final String REDELIVERED = "redelivered";

    /** The extension attribute that holds the {@code content-encoding} property. */
    public static final String CONTENT_ENCODING = "content-encoding";

    /** The extension attribute that holds the {@code expiration} property as received. */
    public static final String EXPIRATION = "expiration";

    /** The extension attribute that holds the {@code user-id} property. */
    public static final String USER_ID = "user-id";

    /** The extension attribute that holds the {@code app-id} property. */
    public static final String APP_ID = "app-id";

    /** The prefix of the extension attributes that hold headers with a structured or timestamp value. */
    public static final String HEADER_PREFIX = "header.";

    private static final String DELIVERY_COUNT = "x-delivery-count";
    private static final String ACQUIRED_COUNT = "x-acquired-count";
    private static final String DEATH = "x-death";
    private static final int PERSISTENT = 2;
    private static final int TRANSIENT = 1;
    private static final int MAX_PRIORITY = 255;

    private MessageCodec() {}

    /** The properties and headers of {@code message}, with its production time read from the system clock. */
    public static AMQP.BasicProperties properties(OutboundMessage message) {
        return properties(message, Clock.systemUTC());
    }

    /**
     * The properties and headers of {@code message}. {@code clock} dates a message with no production time and
     * measures the expiry.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT} for a header RabbitMQ has no
     *     place for, a priority outside 0 to 255, or an expiry already past
     */
    public static AMQP.BasicProperties properties(OutboundMessage message, Clock clock) {
        MessageHeaders h = message.headers();
        refuse(h.key().isPresent(), "key");
        refuse(h.groupId().isPresent(), "group id");
        refuse(h.deduplicationId().isPresent(), "deduplication id");
        refuse(h.deliverAt().isPresent(), "delivery time");
        Attributes extension = message.extensions().getOrDefault(EXTENSION, Attributes.empty());
        AMQP.BasicProperties.Builder out = new AMQP.BasicProperties.Builder();
        h.id().ifPresent(out::messageId);
        h.correlationId().ifPresent(out::correlationId);
        h.replyTo().ifPresent(out::replyTo);
        h.contentType().ifPresent(out::contentType);
        h.type().ifPresent(out::type);
        if (h.priority().isPresent()) {
            int priority = h.priority().getAsInt();
            if (priority < 0 || priority > MAX_PRIORITY) {
                throw RabbitMqErrors.invalid("A RabbitMQ priority is 0 to 255, not " + priority + ".");
            }
            out.priority(priority);
        }
        h.durable().ifPresent(durable -> out.deliveryMode(durable ? PERSISTENT : TRANSIENT));
        out.timestamp(Date.from(h.producedAt().orElseGet(clock::instant).truncatedTo(ChronoUnit.SECONDS)));
        if (h.expiresAt().isPresent()) {
            long millis = Duration.between(clock.instant(), h.expiresAt().get()).toMillis();
            if (millis <= 0) {
                throw RabbitMqErrors.invalid("The message expires in the past.");
            }
            out.expiration(Long.toString(millis));
        } else {
            text(extension, EXPIRATION).ifPresent(out::expiration);
        }
        text(extension, CONTENT_ENCODING).ifPresent(out::contentEncoding);
        text(extension, USER_ID).ifPresent(out::userId);
        text(extension, APP_ID).ifPresent(out::appId);
        Map<String, Object> headers = new LinkedHashMap<>();
        message.properties().forEach((name, value) -> headers.put(name, header(value)));
        extension.asMap().forEach((name, value) -> {
            if (name.startsWith(HEADER_PREFIX)) {
                headers.put(name.substring(HEADER_PREFIX.length()), toAmqp(value));
            }
        });
        if (!headers.isEmpty()) {
            out.headers(headers);
        }
        return out.build();
    }

    /**
     * The body bytes of {@code message}.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT} for a tombstone, and the
     *     mapped failure when a streamed body cannot be read
     */
    public static byte[] body(OutboundMessage message) {
        MessageBody body = message.body();
        if (body.isTombstone()) {
            throw RabbitMqErrors.invalid("A RabbitMQ message takes no tombstone body.");
        }
        Optional<Blob> bytes = body.bytes();
        if (bytes.isPresent()) {
            return bytes.get().toByteArray();
        }
        try (InputStream in = body.openStream()) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, false);
        } catch (UncheckedIOException e) {
            throw RabbitMqErrors.fromIo(e.getCause(), false);
        }
    }

    /** A received message with its whole body; see {@link #message(ResourceId, Envelope, AMQP.BasicProperties, byte[], long)}. */
    public static Message message(ResourceId source, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        return message(source, envelope, properties, body, Long.MAX_VALUE);
    }

    /**
     * The SDK message for a delivery from {@code source}. Its reference is the stream offset when the delivery has
     * one, else the delivery tag, which a receiver that settles by channel generation replaces. A body longer than
     * {@code maxBodyBytes} is cut to it and the message says so in {@code truncated}.
     */
    public static Message message(
            ResourceId source, Envelope envelope, AMQP.BasicProperties properties, byte[] body, long maxBodyBytes) {
        Map<String, Object> raw = properties.getHeaders() == null ? Map.of() : properties.getHeaders();
        MessageHeaders.Builder headers = MessageHeaders.builder()
                .id(properties.getMessageId())
                .correlationId(properties.getCorrelationId())
                .replyTo(properties.getReplyTo())
                .contentType(properties.getContentType())
                .type(properties.getType())
                .priority(properties.getPriority());
        Integer mode = properties.getDeliveryMode();
        if (mode != null) {
            headers.durable(mode == PERSISTENT);
        }
        Date timestamp = properties.getTimestamp();
        if (timestamp != null) {
            headers.producedAt(timestamp.toInstant());
        }
        // A quorum queue counts the returns of a message in x-delivery-count and, on a consumer, in x-acquired-count
        // (RabbitMQ 4 sends only the latter there); the larger is the number of earlier deliveries.
        long earlier = Math.max(count(raw.get(DELIVERY_COUNT)), count(raw.get(ACQUIRED_COUNT)));
        if (earlier > 0) {
            headers.deliveryCount(Math.toIntExact(earlier + 1));
        }
        origin(raw, properties.getMessageId(), headers);
        Map<String, PropertyValue> values = new LinkedHashMap<>();
        Map<String, Object> structured = new LinkedHashMap<>();
        raw.forEach((name, value) -> {
            if (DELIVERY_COUNT.equals(name) || ACQUIRED_COUNT.equals(name) || value == null) {
                return;
            }
            PropertyValue scalar = scalar(value);
            if (scalar != null) {
                values.put(name, scalar);
            } else {
                structured.put(HEADER_PREFIX + name, plain(value));
            }
        });
        Attributes.Builder extension = Attributes.builder();
        structured.forEach(extension::putRaw);
        put(extension, EXCHANGE, envelope.getExchange());
        put(extension, ROUTING_KEY, envelope.getRoutingKey());
        extension.putRaw(REDELIVERED, envelope.isRedeliver());
        put(extension, CONTENT_ENCODING, properties.getContentEncoding());
        put(extension, EXPIRATION, properties.getExpiration());
        put(extension, USER_ID, properties.getUserId());
        put(extension, APP_ID, properties.getAppId());
        Object offset = raw.get(StreamOffsets.HEADER);
        if (offset instanceof Number number) {
            extension.putRaw(StreamOffsets.HEADER, number.longValue());
            values.remove(StreamOffsets.HEADER);
        }
        boolean cut = body.length > maxBodyBytes;
        byte[] kept = cut ? Arrays.copyOf(body, (int) maxBodyBytes) : body;
        String position = offset instanceof Number number
                ? Long.toString(number.longValue())
                : Long.toString(envelope.getDeliveryTag());
        return new Message(
                new MessageRef(source, StreamOffsets.PARTITION, position),
                headers.build(),
                values,
                MessageBody.of(kept),
                cut ? Set.of(MessagePart.BODY) : Set.of(),
                Map.of(EXTENSION, extension.build()));
    }

    private static Object header(PropertyValue value) {
        return value instanceof PropertyValue.OfBytes bytes ? bytes.value().toByteArray() : value.boxed();
    }

    private static void refuse(boolean present, String what) {
        if (present) {
            throw RabbitMqErrors.invalid("A RabbitMQ message takes no " + what + ".");
        }
    }

    private static Optional<String> text(Attributes extension, String name) {
        return extension.raw(name).filter(String.class::isInstance).map(String.class::cast);
    }

    private static void put(Attributes.Builder out, String name, @Nullable String value) {
        if (value != null && !value.isEmpty()) {
            out.putRaw(name, value);
        }
    }

    /** The queue (else the exchange) of the first death record, and the message id the message carried into it. */
    private static void origin(Map<String, Object> raw, @Nullable String id, MessageHeaders.Builder headers) {
        if (raw.get(DEATH) instanceof List<?> deaths && !deaths.isEmpty() && deaths.get(0) instanceof Map<?, ?> first) {
            Object from = first.get("queue") != null ? first.get("queue") : first.get("exchange");
            if (from != null) {
                headers.origin(from.toString());
                headers.originId(id);
            }
        }
    }

    private static @Nullable PropertyValue scalar(Object value) {
        return switch (value) {
            case LongString s -> new PropertyValue.OfString(s.toString());
            case String s -> new PropertyValue.OfString(s);
            case Boolean b -> new PropertyValue.OfBoolean(b);
            case Byte b -> new PropertyValue.OfByte(b);
            case Short s -> new PropertyValue.OfShort(s);
            case Integer i -> new PropertyValue.OfInt(i);
            case Long l -> new PropertyValue.OfLong(l);
            case Float f -> new PropertyValue.OfFloat(f);
            case Double d -> new PropertyValue.OfDouble(d);
            case byte[] bytes -> new PropertyValue.OfBytes(Blob.of(bytes));
            default -> null;
        };
    }

    /** A header value in plain Java types only: strings, numbers, instants, blobs, lists and maps. */
    private static Object plain(Object value) {
        return switch (value) {
            case LongString s -> s.toString();
            case Date d -> d.toInstant();
            case byte[] bytes -> Blob.of(bytes);
            case BigDecimal d -> d;
            case Map<?, ?> map -> {
                Map<String, Object> out = new LinkedHashMap<>();
                map.forEach((k, v) -> {
                    if (v != null) {
                        out.put(String.valueOf(k), plain(v));
                    }
                });
                yield Map.copyOf(out);
            }
            case List<?> list -> {
                List<Object> out = new ArrayList<>();
                list.forEach(v -> {
                    if (v != null) {
                        out.add(plain(v));
                    }
                });
                yield List.copyOf(out);
            }
            default -> value;
        };
    }

    /** The inverse of {@link #plain}: what the AMQP client can write into a header table. */
    private static Object toAmqp(Object value) {
        return switch (value) {
            case Instant i -> Date.from(i);
            case Blob b -> b.toByteArray();
            case Map<?, ?> map -> {
                Map<String, Object> out = new HashMap<>();
                map.forEach((k, v) -> out.put(String.valueOf(k), toAmqp(v)));
                yield out;
            }
            case List<?> list -> list.stream().map(MessageCodec::toAmqp).toList();
            default -> value;
        };
    }

    private static long count(Object value) {
        return value instanceof Number number ? number.longValue() : 0;
    }
}
