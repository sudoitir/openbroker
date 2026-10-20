package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.Attributes;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** A message to send. */
public record OutboundMessage(
        MessageHeaders headers,
        Map<String, PropertyValue> properties,
        MessageBody body,
        Map<String, Attributes> extensions) {

    public OutboundMessage {
        Objects.requireNonNull(headers, "headers");
        properties = Map.copyOf(properties);
        Objects.requireNonNull(body, "body");
        extensions = Map.copyOf(extensions);
    }

    public static OutboundMessage of(MessageBody body) {
        return new OutboundMessage(MessageHeaders.empty(), Map.of(), body, Map.of());
    }

    /**
     * A copy of a message read from the same system, keeping every header, property, the body and
     * the extensions, so a driver can reproduce it exactly.
     */
    public static OutboundMessage copyOf(Message message) {
        return new OutboundMessage(message.headers(), message.properties(), message.body(), message.extensions());
    }

    public OutboundMessage withHeaders(MessageHeaders headers) {
        return new OutboundMessage(headers, properties, body, extensions);
    }

    /** This message with these properties laid over its own. */
    public OutboundMessage withProperties(Map<String, PropertyValue> more) {
        Map<String, PropertyValue> merged = new LinkedHashMap<>(properties);
        merged.putAll(more);
        return new OutboundMessage(headers, merged, body, extensions);
    }
}
