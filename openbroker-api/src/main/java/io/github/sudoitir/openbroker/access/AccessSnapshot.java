package io.github.sudoitir.openbroker.access;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Every capability's assessment at one moment. */
public record AccessSnapshot(Map<Capability, Assessment> assessments, Instant at) {

    public AccessSnapshot {
        assessments = Map.copyOf(assessments);
        Objects.requireNonNull(at, "at");
    }

    public Assessment of(Capability capability) {
        return Optional.ofNullable(assessments.get(capability)).orElseGet(() -> Assessment.unknown("Not assessed"));
    }
}
