package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Reading and filtering the attributes of a request. */
final class Attrs {

    private Attrs() {}

    /** The attributes the projection names; all of them when it names none. */
    static Attributes project(Attributes attributes, Set<String> projection) {
        if (projection.isEmpty()) {
            return attributes;
        }
        Attributes.Builder out = Attributes.builder();
        attributes.asMap().forEach((k, v) -> {
            if (projection.contains(k)) {
                out.putRaw(k, v);
            }
        });
        return out.build();
    }

    /** A whole number attribute, or none; a value that is not a number is refused. */
    static Optional<Long> number(Attributes attributes, String key) {
        return attributes.raw(key).map(v -> {
            if (v instanceof Number n) {
                return n.longValue();
            }
            try {
                return Long.parseLong(String.valueOf(v));
            } catch (NumberFormatException e) {
                throw invalid(key + " must be a whole number.");
            }
        });
    }

    static Optional<String> text(Attributes attributes, String key) {
        return attributes.raw(key).map(String::valueOf);
    }

    /** What changes between two states of a resource, one entry per attribute that differs. */
    static List<AttributeChange> diff(Attributes before, Attributes after) {
        Set<String> keys = new TreeSet<>(before.asMap().keySet());
        keys.addAll(after.asMap().keySet());
        List<AttributeChange> out = new ArrayList<>();
        for (String key : keys) {
            Optional<Object> was = before.raw(key);
            Optional<Object> will = after.raw(key);
            if (!was.equals(will)) {
                out.add(new AttributeChange(key, was, will));
            }
        }
        return out;
    }

    static BrokerException invalid(String message) {
        return KafkaErrors.refused(Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, message);
    }
}
