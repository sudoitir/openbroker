package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.messaging.DeduplicationWindow;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * What the broker's address settings say about a destination: the dedupe window, the dead-letter
 * address and the delivery attempts after which the broker dead-letters. Read once per node and
 * address over management, and remembered; a node without a management endpoint, or an answer that
 * cannot be read, leaves the broker's defaults.
 */
final class Addresses {

    /** The broker's default {@code id-cache-size}. */
    static final long DEFAULT_ID_CACHE_SIZE = 20_000;

    private static final int MAX_KNOWN = 1_000;

    /**
     * An address's settings as far as they could be read.
     *
     * @param idCacheSize the duplicate-id cache size of the address
     * @param maxDeliveryAttempts attempts before the broker dead-letters; -1 never
     * @param deadLetterAddress where the broker dead-letters, when it does
     * @param persistIdCache whether the duplicate-id cache survives a restart
     */
    record Settings(
            OptionalLong idCacheSize,
            OptionalInt maxDeliveryAttempts,
            Optional<String> deadLetterAddress,
            boolean persistIdCache) {

        DeduplicationWindow window() {
            return new DeduplicationWindow(
                    OptionalLong.of(idCacheSize.orElse(DEFAULT_ID_CACHE_SIZE)), Optional.empty(), persistIdCache);
        }
    }

    private final Cluster cluster;
    private final Map<String, Settings> known = new ConcurrentHashMap<>();

    Addresses(Cluster cluster) {
        this.cluster = cluster;
    }

    /** The settings of {@code address} on the node; the defaults when management cannot say. */
    Settings of(NodeHandle node, String address) {
        String key = node.id().value() + '\u0000' + address;
        Settings cached = known.get(key);
        if (cached != null) {
            return cached;
        }
        Settings read = read(node, address);
        if (known.size() >= MAX_KNOWN) {
            known.clear();
        }
        known.put(key, read);
        return read;
    }

    private Settings read(NodeHandle node, String address) {
        Settings defaults = new Settings(OptionalLong.empty(), OptionalInt.empty(), Optional.empty(), true);
        if (node.jolokia().isEmpty()) {
            return defaults;
        }
        try {
            NodeJolokia jolokia = cluster.jolokia(node, Deadlines.current(cluster.options()));
            JsonNode settings = jolokia.execOnBrokerParsed("getAddressSettingsAsJSON(java.lang.String)", address);
            JolokiaResponse broker = jolokia.readBroker("IDCacheSize", "PersistIDCache");
            OptionalLong size = number(settings, "idCacheSize");
            if (size.isEmpty() && broker.ok() && broker.attribute("IDCacheSize") != null) {
                size = number(broker.attribute("IDCacheSize"));
            }
            boolean persist = true;
            JsonNode persisted = broker.ok() ? broker.attribute("PersistIDCache") : null;
            if (persisted != null && persisted.isBoolean()) {
                persist = persisted.asBoolean();
            }
            OptionalLong attempts = number(settings, "maxDeliveryAttempts");
            return new Settings(
                    size,
                    attempts.isPresent() ? OptionalInt.of((int) attempts.getAsLong()) : OptionalInt.empty(),
                    text(settings, "deadLetterAddress"),
                    persist);
        } catch (BrokerException | IllegalStateException e) {
            return defaults;
        }
    }

    private static OptionalLong number(@Nullable JsonNode node, String field) {
        return node == null ? OptionalLong.empty() : number(node.get(field));
    }

    private static OptionalLong number(@Nullable JsonNode value) {
        return value != null && value.isNumber() ? OptionalLong.of(value.asLong()) : OptionalLong.empty();
    }

    private static Optional<String> text(@Nullable JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value != null && value.isString() && !value.asString().isBlank()
                ? Optional.of(value.asString())
                : Optional.empty();
    }
}
