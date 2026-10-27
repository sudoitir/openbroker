package io.github.sudoitir.openbroker;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** The connection options a driver accepts, in the Dapr component-metadata style. */
public record ConnectionSchema(List<OptionSpec> options) {

    /** How an option breaks the schema. */
    public enum Problem {
        /** The driver has no option with this key. */
        UNKNOWN,
        /** The option is required and absent. */
        MISSING,
        /** The value does not parse as the option's type. */
        WRONG_TYPE,
        /** The value is not one of the option's allowed values. */
        NOT_ALLOWED
    }

    /**
     * One option that breaks the schema. {@code detail} says what is wrong in words and never quotes
     * the value, so a violation is safe to log and show whether or not the option is sensitive.
     */
    public record Violation(String key, Problem problem, String detail) {

        public Violation {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(problem, "problem");
            Objects.requireNonNull(detail, "detail");
        }
    }

    public ConnectionSchema {
        options = List.copyOf(options);
    }

    public Optional<OptionSpec> option(String key) {
        return options.stream().filter(o -> o.key().equals(key)).findFirst();
    }

    public boolean isSensitive(String key) {
        return option(key).map(OptionSpec::sensitive).orElse(true);
    }

    /** Every way {@code values} breaks this schema, ordered by key; empty when they fit. */
    public List<Violation> validate(Map<String, String> values) {
        Map<String, Violation> found = new TreeMap<>();
        values.forEach((key, value) -> {
            Optional<OptionSpec> spec = option(key);
            if (spec.isEmpty()) {
                found.put(key, new Violation(key, Problem.UNKNOWN, "is not an option of this driver"));
            } else if (!parses(spec.get().type(), value)) {
                found.put(
                        key,
                        new Violation(
                                key,
                                Problem.WRONG_TYPE,
                                "must be " + describe(spec.get().type())));
            } else if (!spec.get().allowedValues().isEmpty()
                    && !spec.get().allowedValues().contains(value)) {
                found.put(
                        key,
                        new Violation(
                                key,
                                Problem.NOT_ALLOWED,
                                "must be one of " + spec.get().allowedValues()));
            }
        });
        for (OptionSpec spec : options) {
            if (spec.required() && !values.containsKey(spec.key())) {
                found.put(spec.key(), new Violation(spec.key(), Problem.MISSING, "is required"));
            }
        }
        return new ArrayList<>(found.values());
    }

    private static String describe(OptionSpec.Type type) {
        return switch (type) {
            case STRING -> "text";
            case INTEGER -> "an integer";
            case BOOLEAN -> "true or false";
            case DURATION -> "an ISO-8601 duration";
            case URI -> "a URI";
        };
    }

    private static boolean parses(OptionSpec.Type type, String value) {
        try {
            return switch (type) {
                case STRING -> true;
                case INTEGER -> {
                    Long.parseLong(value);
                    yield true;
                }
                case BOOLEAN -> value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false");
                case DURATION -> {
                    Duration.parse(value);
                    yield true;
                }
                case URI -> {
                    new URI(value);
                    yield true;
                }
            };
        } catch (NumberFormatException | DateTimeParseException | URISyntaxException _) {
            return false;
        }
    }
}
