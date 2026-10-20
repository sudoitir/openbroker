package io.github.sudoitir.openbroker.model;

import java.util.Objects;
import java.util.Optional;

/**
 * One attribute of a kind's schema.
 *
 * @param requiresReplace changing it needs the resource to be deleted and created again
 */
public record AttributeSpec(
        String key,
        AttributeCategory category,
        boolean writable,
        boolean required,
        boolean immutable,
        boolean requiresReplace,
        Optional<Measure> measure) {

    public AttributeSpec {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(measure, "measure");
        if (category == AttributeCategory.METRIC && measure.isEmpty()) {
            throw new IllegalArgumentException("A metric attribute needs a measure: " + key);
        }
    }

    public static AttributeSpec config(String key, boolean writable, boolean requiresReplace) {
        return new AttributeSpec(
                key, AttributeCategory.CONFIG, writable, false, !writable, requiresReplace, Optional.empty());
    }

    public static AttributeSpec status(String key) {
        return new AttributeSpec(key, AttributeCategory.STATUS, false, false, false, false, Optional.empty());
    }

    public static AttributeSpec metric(String key, Instrument instrument, String unit) {
        return new AttributeSpec(
                key, AttributeCategory.METRIC, false, false, false, false, Optional.of(new Measure(instrument, unit)));
    }
}
