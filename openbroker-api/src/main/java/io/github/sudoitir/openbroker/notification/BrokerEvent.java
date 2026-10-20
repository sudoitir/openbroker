package io.github.sudoitir.openbroker.notification;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Something that happened on a broker, shaped like a CloudEvent: an id, its source node, a type
 * (standard ones in {@link EventTypes}, native ones namespaced), the resource it is about, when it
 * happened, and its data.
 */
public record BrokerEvent(
        String id, NodeId source, String type, Optional<ResourceId> subject, Instant time, Attributes data) {

    public BrokerEvent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(data, "data");
    }
}
