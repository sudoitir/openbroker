package io.github.sudoitir.openbroker.model;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** An immutable map of attribute names to values, read through typed {@link AttributeKey}s. */
public final class Attributes {

    private static final Attributes EMPTY = new Attributes(Map.of());

    private final Map<String, Object> values;

    private Attributes(Map<String, Object> values) {
        this.values = values;
    }

    public static Attributes empty() {
        return EMPTY;
    }

    public static Attributes of(Map<String, ?> values) {
        return values.isEmpty() ? EMPTY : new Attributes(Map.copyOf(values));
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> Optional<T> get(AttributeKey<T> key) {
        Object v = values.get(key.name());
        return key.type().isInstance(v) ? Optional.of(key.type().cast(v)) : Optional.empty();
    }

    public Optional<Object> raw(String name) {
        return Optional.ofNullable(values.get(name));
    }

    public Map<String, Object> asMap() {
        return values;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    /** These attributes with {@code other}'s laid over them. */
    public Attributes merge(Attributes other) {
        Map<String, Object> merged = new LinkedHashMap<>(values);
        merged.putAll(other.values);
        return of(merged);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Attributes other && values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return values.toString();
    }

    public static final class Builder {
        private final Map<String, Object> values = new LinkedHashMap<>();

        private Builder() {}

        public <T> Builder put(AttributeKey<T> key, T value) {
            values.put(key.name(), Objects.requireNonNull(value, key.name()));
            return this;
        }

        public Builder putRaw(String name, Object value) {
            values.put(name, Objects.requireNonNull(value, name));
            return this;
        }

        public Builder putAll(Attributes attributes) {
            values.putAll(attributes.values);
            return this;
        }

        public Attributes build() {
            return of(values);
        }
    }
}
