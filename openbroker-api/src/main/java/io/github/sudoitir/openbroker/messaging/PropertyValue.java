package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/** A typed message property value. Every system's property types map onto these. */
public sealed interface PropertyValue {

    record OfBoolean(boolean value) implements PropertyValue {}

    record OfByte(byte value) implements PropertyValue {}

    record OfShort(short value) implements PropertyValue {}

    record OfInt(int value) implements PropertyValue {}

    record OfLong(long value) implements PropertyValue {}

    record OfFloat(float value) implements PropertyValue {}

    record OfDouble(double value) implements PropertyValue {}

    record OfString(String value) implements PropertyValue {
        public OfString {
            Objects.requireNonNull(value, "value");
        }
    }

    record OfBytes(Blob value) implements PropertyValue {
        public OfBytes {
            Objects.requireNonNull(value, "value");
        }
    }

    static PropertyValue of(boolean value) {
        return new OfBoolean(value);
    }

    static PropertyValue of(int value) {
        return new OfInt(value);
    }

    static PropertyValue of(long value) {
        return new OfLong(value);
    }

    static PropertyValue of(double value) {
        return new OfDouble(value);
    }

    static PropertyValue of(String value) {
        return new OfString(value);
    }

    /** The value as Java sees it: a boxed primitive, a {@link String} or a {@link Blob}. */
    default Object boxed() {
        return switch (this) {
            case OfBoolean v -> v.value();
            case OfByte v -> v.value();
            case OfShort v -> v.value();
            case OfInt v -> v.value();
            case OfLong v -> v.value();
            case OfFloat v -> v.value();
            case OfDouble v -> v.value();
            case OfString v -> v.value();
            case OfBytes v -> v.value();
        };
    }

    /** The value as text, as a filter or a display compares it. */
    default String asText() {
        return String.valueOf(boxed());
    }
}
