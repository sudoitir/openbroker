package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

/**
 * A query's name and field selector, as the broker's own list filter where it can take them. A
 * {@code listX} filter holds one predicate: an exact name goes to the broker first, else the first
 * selector field the kind can filter on; everything else is applied to the rows that come back. A
 * selector value of an id-valued attribute is the resource id's text.
 */
final class Filters {

    /** Per kind, the attribute or name ({@code ""}) to the list field the broker filters on. */
    private static final Map<KindId, Map<String, String>> FIELDS = Map.of(
            ArtemisKinds.QUEUE, Map.of("", "name", StandardAttributes.DESTINATION.name(), "address"),
            ArtemisKinds.ADDRESS, Map.of("", "name"),
            ArtemisKinds.CONNECTION,
                    Map.of(
                            "",
                            "connectionID",
                            StandardAttributes.CLIENT_ID.name(),
                            "clientID",
                            StandardAttributes.REMOTE_ADDRESS.name(),
                            "remoteAddress"),
            ArtemisKinds.SESSION, Map.of("", "id", StandardAttributes.PARENT.name(), "connectionID"),
            ArtemisKinds.CONSUMER,
                    Map.of(
                            "",
                            "id",
                            StandardAttributes.PARENT.name(),
                            "session",
                            StandardAttributes.SUBSCRIPTION.name(),
                            "queue",
                            StandardAttributes.DESTINATION.name(),
                            "address"),
            ArtemisKinds.PRODUCER,
                    Map.of(
                            "",
                            "id",
                            StandardAttributes.PARENT.name(),
                            "session",
                            StandardAttributes.DESTINATION.name(),
                            "address"));

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private Filters() {}

    /** The {@code listX} options for a query: one pushed-down predicate, or every row. */
    static String options(KindId kind, ResourceQuery query) {
        Map<String, String> fields = FIELDS.getOrDefault(kind, Map.of());
        Optional<String> exactName = query.namePattern().filter(p -> !p.contains("*") && !p.contains("?"));
        if (exactName.isPresent() && fields.containsKey("")) {
            return equals(fields.get(""), exactName.get());
        }
        for (Map.Entry<String, String> e : query.fieldSelector().entrySet()) {
            String field = fields.get(e.getKey());
            if (field != null) {
                return equals(field, value(e.getValue()));
            }
        }
        return ArtemisLists.ALL;
    }

    /** The rows that match the whole query, whatever the broker already filtered. */
    static List<Resource> keep(ResourceQuery query, List<Resource> rows) {
        Predicate<String> names = Queries.names(query);
        return rows.stream()
                .filter(r -> names.test(r.id().name()))
                .filter(r ->
                        query.fieldSelector().entrySet().stream().allMatch(e -> matches(r, e.getKey(), e.getValue())))
                .toList();
    }

    private static boolean matches(Resource r, String attribute, String expected) {
        Object actual = r.attributes().raw(attribute).orElse(null);
        return actual != null && Objects.equals(text(actual), expected);
    }

    private static String text(Object value) {
        return value.toString();
    }

    /** An id-valued selector names the resource by its id; the broker filters on its last segment. */
    private static String value(String selector) {
        @Nullable ResourceId id = parse(selector);
        return id != null ? id.name() : selector;
    }

    private static @Nullable ResourceId parse(String text) {
        try {
            return ResourceId.parse(text);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** The broker's filter document, in the field order its list operations read. */
    private static String equals(String field, String value) {
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put("field", field);
        filter.put("operation", "EQUALS");
        filter.put("value", value);
        return JSON.writeValueAsString(filter);
    }
}
