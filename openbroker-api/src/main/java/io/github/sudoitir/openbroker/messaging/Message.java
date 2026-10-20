package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.Attributes;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A message as read: where it is, its headers, its typed properties, its body, what was cut short,
 * and system-specific extensions keyed by namespace ({@code jms}, {@code artemis}).
 */
public record Message(
        MessageRef ref,
        MessageHeaders headers,
        Map<String, PropertyValue> properties,
        MessageBody body,
        Set<MessagePart> truncated,
        Map<String, Attributes> extensions) {

    public Message {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(headers, "headers");
        properties = Map.copyOf(properties);
        Objects.requireNonNull(body, "body");
        truncated = Set.copyOf(truncated);
        extensions = Map.copyOf(extensions);
    }

    public Attributes extension(String namespace) {
        return extensions.getOrDefault(namespace, Attributes.empty());
    }
}
