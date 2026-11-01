package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

/** Small readers of the management API's JSON that the families share. */
final class ReadSupport {

    private ReadSupport() {}

    /** The non-empty text of {@code field}, or none. Numbers read as their text. */
    static Optional<String> text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isString() || value.isNumber()) {
            String text = value.asString();
            return text.isEmpty() ? Optional.empty() : Optional.of(text);
        }
        return Optional.empty();
    }

    /** The field {@code path} (dot separated) of {@code node}, missing when any step is. */
    static JsonNode at(JsonNode node, String path) {
        JsonNode current = node;
        for (String step : path.split("\\.")) {
            current = current.path(step);
        }
        return current;
    }

    static boolean flag(JsonNode node, String field) {
        return node.path(field).asBoolean(false);
    }

    /** The value of a JSON scalar as a Java object; anything else as JSON text. */
    static Object scalar(JsonNode value) {
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isIntegralNumber()) {
            return value.asLong();
        }
        if (value.isNumber()) {
            return value.asDouble();
        }
        if (value.isString()) {
            return value.asString();
        }
        return value.toString();
    }

    /** Puts every member of {@code object} under {@code prefix + name}. */
    static void members(Attributes.Builder out, String prefix, JsonNode object) {
        for (Map.Entry<String, JsonNode> member : object.properties()) {
            out.putRaw(prefix + member.getKey(), scalar(member.getValue()));
        }
    }

    /** Tags as the management API takes them: a list or a comma separated text, as comma separated text. */
    static Optional<String> tags(JsonNode value) {
        if (value.isArray()) {
            List<String> tags = new ArrayList<>();
            value.forEach(t -> tags.add(t.asString()));
            return Optional.of(String.join(",", tags));
        }
        return value.isString() ? Optional.of(value.asString()) : Optional.empty();
    }

    /** The members of a list of names. */
    static List<String> names(JsonNode list) {
        List<String> out = new ArrayList<>();
        list.forEach(n -> out.add(n.asString()));
        return out;
    }

    static void put(Attributes.Builder out, String key, Optional<?> value) {
        value.ifPresent(v -> out.putRaw(key, v));
    }

    static void putLong(Attributes.Builder out, String key, JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isNumber()) {
            out.putRaw(key, value.asLong());
        }
    }

    /** The parent of a resource that lives in a virtual host. */
    static void parent(Attributes.Builder out, String vhost) {
        out.put(StandardAttributes.PARENT, ResourceId.of(RabbitMqKinds.VHOST.value(), vhost));
    }

    /** The placement of what has no node of its own: the cluster. */
    static List<Placement> onCluster() {
        return List.of(Placement.instance(RabbitMqCluster.CLUSTER, Attributes.empty()));
    }

    private static final Pattern URI_PARTS = Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*)://(?:[^@/?#]*@)?([^?#]*).*$");

    /**
     * {@code uri} without its user information, query and fragment, so that no credential or key file path reaches an
     * attribute. Text that is not a URI at all becomes {@code amqp://unknown}.
     */
    static URI redacted(String uri) {
        Matcher parts = URI_PARTS.matcher(uri);
        if (parts.matches()) {
            try {
                return new URI(parts.group(1) + "://" + parts.group(2));
            } catch (URISyntaxException _) {
                // Falls through to the placeholder below: nothing of a malformed address is worth keeping.
            }
        }
        return URI.create("amqp://unknown");
    }

    /** Whether {@code uri} names no host, which an {@code amqp://} address does for the cluster that holds it. */
    static boolean isLocal(String uri) {
        Matcher parts = URI_PARTS.matcher(uri);
        return !parts.matches() || parts.group(2).isEmpty() || parts.group(2).startsWith("/");
    }

    /** The virtual host that a local {@code amqp:///vhost} address names, or {@code fallback} when it names none. */
    static String localVhost(String uri, String fallback) {
        Matcher parts = URI_PARTS.matcher(uri);
        if (parts.matches() && parts.group(2).startsWith("/") && parts.group(2).length() > 1) {
            return URLDecoder.decode(parts.group(2).substring(1), StandardCharsets.UTF_8);
        }
        return fallback;
    }

    /** A connection state as a condition: true while it is {@code running}, false with the state as the reason. */
    static Condition connected(String state) {
        boolean running = "running".equals(state);
        return new Condition(
                ConditionTypes.CONNECTED,
                running ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                running ? "" : state,
                running ? "" : "The link is " + state + ".",
                Optional.empty());
    }
}
