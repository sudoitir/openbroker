package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * The rows of a queue's {@code browse(int,int,String)}, as messages. The value is a plain JSON array
 * with typed scalars. The broker cuts an oversized text body or string property at {@code
 * management-message-attribute-size-limit} and appends {@code , + N more}; a bytes body arrives as a
 * {@code BodyPreview} cut at that limit with no marker, so the limit is the only sign it was cut.
 */
public final class BrowseRows {

    /** The broker's own truncation suffix, for instance {@code ", + 3744 more"}. */
    static final Pattern TRUNCATION_MARKER = Pattern.compile(", \\+ \\d+ more$");

    /** The default attribute size limit, assumed when the address settings could not be read. */
    static final int DEFAULT_ATTRIBUTE_SIZE_LIMIT = 256;

    /** The properties the broker keeps headers in; they are read as headers, not properties. */
    static final String CORRELATION_ID = "JMSCorrelationID";

    static final String TYPE = "JMSType";
    static final String REPLY_TO = "JMSReplyTo";
    static final String GROUP_ID = "_AMQ_GROUP_ID";
    static final String DUPLICATE_ID = "_AMQ_DUPL_ID";
    static final String SCHEDULED_DELIVERY = "_AMQ_SCHED_DELIVERY";

    private static final Set<String> HEADER_PROPERTIES =
            Set.of(CORRELATION_ID, TYPE, REPLY_TO, GROUP_ID, DUPLICATE_ID, SCHEDULED_DELIVERY);

    /** Extension keys under the {@code artemis} namespace. */
    public static final String MESSAGE_TYPE = "artemis.message_type";

    public static final String PERSISTENT_SIZE = "artemis.persistent_size";
    public static final String ADDRESS = "artemis.address";
    public static final String CUT_AT = "artemis.cut_at";

    private BrowseRows() {}

    /**
     * One row as a message.
     *
     * @param sizeLimit the limit the broker cut values at, or -1 when it cuts nothing
     * @param maxBodyBytes the most body bytes the caller wants
     */
    static Message parse(JsonNode row, ResourceId subscription, NodeId node, int sizeLimit, long maxBodyBytes) {
        Set<MessagePart> truncated = EnumSet.noneOf(MessagePart.class);
        Map<String, PropertyValue> properties = new LinkedHashMap<>();
        Map<String, String> headerValues = new LinkedHashMap<>();
        Integer propertyCut = typed(row, "StringProperties", v -> PropertyValue.of(v.asString()), properties);
        typed(row, "IntProperties", v -> PropertyValue.of(v.asInt()), properties);
        typed(row, "ShortProperties", v -> new PropertyValue.OfShort((short) v.asInt()), properties);
        typed(row, "ByteProperties", v -> new PropertyValue.OfByte((byte) v.asInt()), properties);
        typed(row, "LongProperties", v -> PropertyValue.of(v.asLong()), properties);
        typed(row, "DoubleProperties", v -> PropertyValue.of(v.asDouble()), properties);
        typed(row, "FloatProperties", v -> new PropertyValue.OfFloat((float) v.asDouble()), properties);
        typed(row, "BooleanProperties", v -> PropertyValue.of(v.asBoolean()), properties);
        if (propertyCut != null) {
            truncated.add(MessagePart.PROPERTIES);
        }
        for (String name : HEADER_PROPERTIES) {
            PropertyValue value = properties.remove(name);
            if (value != null) {
                headerValues.put(name, value.asText());
            }
        }

        byte[] body;
        Integer cutAt = null;
        String text = text(row, "text");
        JsonNode preview = row.get("BodyPreview");
        if (text != null) {
            Matcher marker = TRUNCATION_MARKER.matcher(text);
            if (marker.find()) {
                cutAt = marker.start();
                text = text.substring(0, marker.start());
            }
            body = text.getBytes(StandardCharsets.UTF_8);
        } else if (preview != null && preview.isArray()) {
            body = new byte[preview.size()];
            for (int i = 0; i < body.length; i++) {
                body[i] = (byte) preview.get(i).asInt();
            }
            if (sizeLimit >= 0 && body.length >= sizeLimit) {
                cutAt = sizeLimit;
            }
        } else {
            body = new byte[0];
        }
        if (cutAt != null) {
            truncated.add(MessagePart.BODY);
        }
        if (body.length > maxBodyBytes) {
            body = Arrays.copyOf(body, (int) maxBodyBytes);
            truncated.add(MessagePart.BODY);
        }

        MessageHeaders.Builder headers = MessageHeaders.builder()
                .id(blankToNull(text(row, "userID")))
                .correlationId(first(text(row, "correlationID"), headerValues.get(CORRELATION_ID)))
                .type(headerValues.get(TYPE))
                .replyTo(headerValues.get(REPLY_TO))
                .groupId(first(text(row, "groupID"), headerValues.get(GROUP_ID)))
                .deduplicationId(headerValues.get(DUPLICATE_ID))
                .priority((int) num(row, "priority"))
                .durable(row.path("durable").asBoolean(false));
        long timestamp = num(row, "timestamp");
        if (timestamp > 0) {
            headers.producedAt(Instant.ofEpochMilli(timestamp));
        }
        long expiration = num(row, "expiration");
        if (expiration > 0) {
            headers.expiresAt(Instant.ofEpochMilli(expiration));
        }
        String scheduled = headerValues.get(SCHEDULED_DELIVERY);
        if (scheduled != null) {
            headers.deliverAt(Instant.ofEpochMilli(Long.parseLong(scheduled)));
        }

        Attributes.Builder artemis = Attributes.builder()
                .putRaw(MESSAGE_TYPE, (int) num(row, "type"))
                .putRaw(PERSISTENT_SIZE, num(row, "persistentSize"));
        String address = text(row, "address");
        if (address != null) {
            artemis.putRaw(ADDRESS, address);
        }
        Integer cut = cutAt != null ? cutAt : propertyCut;
        if (cut != null) {
            artemis.putRaw(CUT_AT, cut);
        }
        return new Message(
                new MessageRef(subscription, Shard.node(node), String.valueOf(num(row, "messageID"))),
                headers.build(),
                properties,
                MessageBody.of(Blob.of(body)),
                truncated,
                Map.of("artemis", artemis.build()));
    }

    /** Adds one typed property map; returns where the broker cut the first string it cut, or null. */
    private static @Nullable Integer typed(
            JsonNode row, String field, Function<JsonNode, PropertyValue> as, Map<String, PropertyValue> into) {
        JsonNode node = row.get(field);
        Integer cut = null;
        if (node != null && node.isObject()) {
            for (Map.Entry<String, JsonNode> e : node.properties()) {
                into.put(e.getKey(), as.apply(e.getValue()));
                if (cut == null && e.getValue().isString()) {
                    Matcher marker = TRUNCATION_MARKER.matcher(e.getValue().asString());
                    if (marker.find()) {
                        cut = marker.start();
                    }
                }
            }
        }
        return cut;
    }

    private static long num(JsonNode row, String field) {
        JsonNode v = row.get(field);
        if (v == null || v.isNull()) {
            return 0L;
        }
        if (v.isNumber()) {
            return v.asLong();
        }
        try {
            return Long.parseLong(v.asString().trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static @Nullable String text(JsonNode row, String field) {
        JsonNode v = row.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }

    private static @Nullable String blankToNull(@Nullable String v) {
        return v == null || v.isBlank() ? null : v;
    }

    private static @Nullable String first(@Nullable String a, @Nullable String b) {
        return a != null && !a.isBlank() ? a : b;
    }
}
