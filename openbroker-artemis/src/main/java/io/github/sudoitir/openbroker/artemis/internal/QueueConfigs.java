package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Between a queue's attributes and Artemis' {@code QueueConfiguration} document. Standard attributes
 * map to their configuration keys; any other configuration key is given as {@code artemis.<key>} with
 * underscores ({@code artemis.last_value_key} for {@code last-value-key}).
 */
public final class QueueConfigs {

    /** Queue MBean attribute to configuration key, read back in full before every update. */
    static final Map<String, String> ATTRIBUTE_TO_KEY = Map.ofEntries(
            Map.entry("Name", "name"),
            Map.entry("Address", "address"),
            Map.entry("RoutingType", "routing-type"),
            Map.entry("Filter", "filter-string"),
            Map.entry("Durable", "durable"),
            Map.entry("User", "user"),
            Map.entry("MaxConsumers", "max-consumers"),
            Map.entry("Exclusive", "exclusive"),
            Map.entry("GroupRebalance", "group-rebalance"),
            Map.entry("GroupRebalancePauseDispatch", "group-rebalance-pause-dispatch"),
            Map.entry("GroupBuckets", "group-buckets"),
            Map.entry("GroupFirstKey", "group-first-key"),
            Map.entry("LastValue", "last-value"),
            Map.entry("LastValueKey", "last-value-key"),
            Map.entry("NonDestructive", "non-destructive"),
            Map.entry("PurgeOnNoConsumers", "purge-on-no-consumers"),
            Map.entry("Enabled", "enabled"),
            Map.entry("ConsumersBeforeDispatch", "consumers-before-dispatch"),
            Map.entry("DelayBeforeDispatch", "delay-before-dispatch"),
            Map.entry("ConsumerPriority", "consumer-priority"),
            Map.entry("AutoDelete", "auto-delete"),
            Map.entry("RingSize", "ring-size"),
            Map.entry("ConfigurationManaged", "configuration-managed"),
            Map.entry("Temporary", "temporary"));

    /** Keys the broker will not change on a live queue. */
    static final Set<String> IMMUTABLE = Set.of("name", "address", "routing-type", "durable");

    private static final Set<String> KEYS = Set.copyOf(ATTRIBUTE_TO_KEY.values());

    private QueueConfigs() {}

    /** The configuration document for a queue named {@code name} with these attributes. */
    static Map<String, Object> toConfig(String name, Attributes attributes, boolean forCreate) {
        Map<String, Object> config = new LinkedHashMap<>();
        if (forCreate) {
            config.put("name", name);
            config.put("routing-type", "ANYCAST");
            config.put("durable", true);
            config.put("address", name);
        }
        attributes.asMap().forEach((key, value) -> {
            switch (key) {
                case "openbroker.destination" -> config.put("address", ((ResourceId) value).name());
                case "openbroker.durable" -> config.put("durable", value);
                case "openbroker.temporary" -> config.put("temporary", value);
                case "openbroker.filter" -> config.put("filter-string", value);
                case "openbroker.max_consumers" -> config.put("max-consumers", value);
                case "openbroker.dead_letter" ->
                    // Artemis keeps a queue's dead-letter address in the address settings that match it,
                    // which this driver does not write; accepting the attribute would ignore it.
                    throw Queries.invalid("This driver cannot set an Artemis queue's dead-letter address yet;"
                            + " set it in the broker's address settings.");
                case ArtemisKinds.ROUTING_TYPE ->
                    config.put("routing-type", String.valueOf(value).toUpperCase(Locale.ROOT));
                case ArtemisKinds.AUTO_CREATE_ADDRESS -> {
                    if (!forCreate) {
                        throw Queries.invalid("Whether a queue creates its address is decided when it is created.");
                    }
                    config.put("auto-create-address", value);
                }
                default -> config.put(nativeKey(key), value);
            }
        });
        return config;
    }

    /** The configuration key an {@code artemis.} attribute names; anything else is refused. */
    private static String nativeKey(String attribute) {
        if (attribute.startsWith("artemis.")) {
            String key = attribute.substring("artemis.".length()).replace('_', '-');
            if (KEYS.contains(key)) {
                return key;
            }
        }
        throw Queries.invalid("An Artemis queue has no attribute " + attribute + ".");
    }

    /** A queue MBean's attributes, read in full, as a configuration document. */
    static Map<String, Object> fromMBean(@Nullable JsonNode value, ObjectMapper mapper) {
        Map<String, Object> config = new LinkedHashMap<>();
        if (value == null || !value.isObject()) {
            return config;
        }
        ATTRIBUTE_TO_KEY.forEach((attribute, key) -> {
            JsonNode node = value.get(attribute);
            if (node != null && !node.isNull()) {
                config.put(key, mapper.convertValue(node, Object.class));
            }
        });
        return config;
    }

    /** The requested keys whose values the existing queue does not have. */
    static List<String> differences(Map<String, Object> requested, Map<String, Object> existing) {
        // Only what the broker reports back can be compared; a create directive is not a property.
        return requested.entrySet().stream()
                .filter(e -> KEYS.contains(e.getKey()))
                .filter(e -> !same(e.getValue(), existing.get(e.getKey())))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    private static boolean same(@Nullable Object a, @Nullable Object b) {
        if (a == null || b == null) {
            return a == b || "".equals(a) || "".equals(b);
        }
        if (a instanceof Number x && b instanceof Number y) {
            return x.longValue() == y.longValue();
        }
        return String.valueOf(a).equalsIgnoreCase(String.valueOf(b));
    }
}
