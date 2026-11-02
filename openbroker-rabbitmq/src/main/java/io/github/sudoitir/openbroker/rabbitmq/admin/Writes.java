package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The calls that read one object and change it through the management API, the attribute mapping both ways and the
 * etag fingerprint of an object's state. Nothing here decides whether an operation may run: {@link RabbitMqOperations}
 * does, and calls these once it has.
 */
final class Writes {

    private static final String API = "/api/";
    private static final String QUEUES = API + "queues/";
    private static final String EXCHANGES = API + "exchanges/";
    private static final String BINDINGS = API + "bindings/";
    private static final String POLICIES = API + "policies/";
    private static final String OPERATOR_POLICIES = API + "operator-policies/";
    private static final String PERMISSIONS = API + "permissions/";
    private static final String TOPIC_PERMISSIONS = API + "topic-permissions/";
    private static final String SHOVELS = API + "parameters/shovel/";
    private static final String DURABLE = StandardAttributes.DURABLE.name();
    private static final String FILTER = StandardAttributes.FILTER.name();
    private static final String SHOVEL_DEFINITION = "rabbitmq.definition";
    private static final String QUEUE_TYPE_ARGUMENT = "x-queue-type";
    private static final String ARGUMENTS = "arguments";
    private static final String TOPIC_FIELD = "exchange";
    private static final String ABSENT = "absent";
    private static final String AUTO_DELETE_FIELD = "auto_delete";
    private static final String DURABLE_FIELD = "durable";
    private static final String WRITE_FIELD = "write";

    private final RabbitMqCluster cluster;

    Writes(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    // Paths.

    /** The path of the object {@code id} names. */
    static String objectPath(ResourceId id) {
        List<String> s = id.path().segments();
        KindId kind = id.kind();
        if (kind.equals(RabbitMqKinds.VHOST)) {
            return API + "vhosts/" + seg(id, 1, 0);
        }
        if (kind.equals(RabbitMqKinds.CONNECTION)) {
            return API + "connections/" + seg(id, 1, 0);
        }
        if (kind.equals(RabbitMqKinds.EXCHANGE)) {
            return EXCHANGES + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        if (isQueue(kind)) {
            return QUEUES + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        if (kind.equals(RabbitMqKinds.BINDING)) {
            return BINDINGS + seg(id, 4, 0) + "/e/" + seg(id, 4, 1) + "/q/" + seg(id, 4, 2) + "/" + seg(id, 4, 3);
        }
        if (kind.equals(RabbitMqKinds.EXCHANGE_BINDING)) {
            return BINDINGS + seg(id, 4, 0) + "/e/" + seg(id, 4, 1) + "/e/" + seg(id, 4, 2) + "/" + seg(id, 4, 3);
        }
        if (kind.equals(RabbitMqKinds.POLICY)) {
            return POLICIES + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        if (kind.equals(RabbitMqKinds.OPERATOR_POLICY)) {
            return OPERATOR_POLICIES + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        if (kind.equals(RabbitMqKinds.PERMISSION)) {
            return PERMISSIONS + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        if (kind.equals(RabbitMqKinds.TOPIC_PERMISSION)) {
            return TOPIC_PERMISSIONS + seg(id, 3, 0) + "/" + seg(id, 3, 1);
        }
        if (kind.equals(RabbitMqKinds.SHOVEL)) {
            return SHOVELS + seg(id, 2, 0) + "/" + seg(id, 2, 1);
        }
        throw RabbitMqErrors.invalid(kind + " has no object the driver changes (" + s + ").");
    }

    /** The path a binding is created at: the same without its properties key, which the server assigns. */
    static String bindingCollection(ResourceId id) {
        String path = objectPath(id);
        return path.substring(0, path.lastIndexOf('/'));
    }

    private static String seg(ResourceId id, int expected, int index) {
        List<String> s = id.path().segments();
        if (s.size() != expected) {
            throw RabbitMqErrors.invalid(
                    id.kind() + " is named by " + expected + " id segment(s), not " + s.size() + ".");
        }
        return ManagementPaths.segment(s.get(index));
    }

    static boolean isQueue(KindId kind) {
        return kind.equals(RabbitMqKinds.CLASSIC_QUEUE)
                || kind.equals(RabbitMqKinds.QUORUM_QUEUE)
                || kind.equals(RabbitMqKinds.STREAM);
    }

    private static String queueType(KindId kind) {
        if (kind.equals(RabbitMqKinds.QUORUM_QUEUE)) {
            return "quorum";
        }
        return kind.equals(RabbitMqKinds.STREAM) ? "stream" : "classic";
    }

    // Reads.

    /** The object {@code id} names, or empty when the server has none. */
    Optional<JsonNode> read(ResourceId id, Deadline deadline) {
        JsonNode body;
        try {
            body = cluster.management()
                    .get(objectPath(id), Map.of(), deadline, "read " + id.kind())
                    .body();
        } catch (BrokerException e) {
            if (ErrorReasons.RESOURCE_GONE.equals(e.reason())) {
                return Optional.empty();
            }
            throw e;
        }
        if (id.kind().equals(RabbitMqKinds.TOPIC_PERMISSION)) {
            String exchange = ManagementPaths.serverName(id.path().segments().get(2));
            for (JsonNode item : body) {
                if (exchange.equals(item.path(TOPIC_FIELD).asString())) {
                    return Optional.of(item);
                }
            }
            return Optional.empty();
        }
        return body.isObject() ? Optional.of(body) : Optional.empty();
    }

    /** The queues of {@code vhost} with their {@code name}, {@code messages} and {@code consumers}. */
    List<JsonNode> queues(String vhost, Deadline deadline) {
        List<JsonNode> out = new ArrayList<>();
        int page = 1;
        int pages;
        do {
            JsonNode body = cluster.management()
                    .get(
                            QUEUES + ManagementPaths.segment(vhost),
                            Map.of(
                                    "columns",
                                    "name,messages,consumers",
                                    "page",
                                    Integer.toString(page),
                                    "page_size",
                                    Integer.toString(Listing.MAX_PAGE_SIZE)),
                            deadline,
                            "list queues")
                    .body();
            JsonNode items = body.isArray() ? body : body.path("items");
            items.forEach(out::add);
            pages = body.isArray() ? 1 : body.path("page_count").asInt(1);
            page++;
        } while (page <= pages);
        return out;
    }

    /** How many bindings have the exchange as their source. */
    int bindingsFrom(String vhost, String exchange, Deadline deadline) {
        JsonNode body = cluster.management()
                .get(
                        EXCHANGES + ManagementPaths.segment(vhost) + "/" + ManagementPaths.segment(exchange)
                                + "/bindings/source",
                        Map.of(),
                        deadline,
                        "list bindings")
                .body();
        return body.size();
    }

    /** How many topic permissions the user of {@code id} has in its virtual host. */
    int topicPermissions(ResourceId id, Deadline deadline) {
        return cluster.management()
                .get(objectPath(id), Map.of(), deadline, "list topic permissions")
                .body()
                .size();
    }

    /** Whether a static shovel (one from configuration, which no call removes) has the name. */
    boolean staticShovel(String vhost, String name, Deadline deadline) {
        JsonNode body = cluster.management()
                .get(API + "shovels/" + ManagementPaths.segment(vhost), Map.of(), deadline, "list shovels")
                .body();
        for (JsonNode item : body) {
            if (name.equals(item.path("name").asString())
                    && "static".equals(item.path("type").asString())) {
                return true;
            }
        }
        return false;
    }

    // Writes.

    /** Creates or replaces the object with {@code attributes}: a PUT, or a POST for a binding. */
    void put(ResourceId id, Map<String, Object> attributes, Deadline deadline) {
        JsonNode body = body(id, attributes);
        if (id.kind().equals(RabbitMqKinds.BINDING) || id.kind().equals(RabbitMqKinds.EXCHANGE_BINDING)) {
            cluster.management().post(bindingCollection(id), body, deadline, "create " + id.kind());
        } else {
            cluster.management().put(objectPath(id), body, deadline, "put " + id.kind());
        }
    }

    /** Deletes the object, with the server's own conditions as {@code query}. */
    void delete(ResourceId id, Map<String, String> query, Deadline deadline) {
        cluster.management().delete(objectPath(id), query, deadline, "delete " + id.kind());
    }

    /** Removes the messages a queue holds ready. */
    void purge(ResourceId id, Deadline deadline) {
        cluster.management().delete(objectPath(id) + "/contents", Map.of(), deadline, "purge " + id.kind());
    }

    // Attributes and bodies.

    /** The configuration attributes of the object whose management JSON is {@code json}. */
    static Map<String, Object> attributes(KindId kind, JsonNode json) {
        Map<String, Object> out = new TreeMap<>();
        if (kind.equals(RabbitMqKinds.VHOST)) {
            put(out, RabbitMqKinds.DESCRIPTION, json.path("description"));
            out.put(RabbitMqKinds.TAGS, tags(json.path("tags")));
            put(out, RabbitMqKinds.DEFAULT_QUEUE_TYPE, json.path("default_queue_type"));
        } else if (kind.equals(RabbitMqKinds.EXCHANGE)) {
            put(out, RabbitMqKinds.EXCHANGE_TYPE, json.path("type"));
            put(out, DURABLE, json.path(DURABLE_FIELD));
            put(out, RabbitMqKinds.AUTO_DELETE, json.path(AUTO_DELETE_FIELD));
            put(out, RabbitMqKinds.INTERNAL, json.path("internal"));
            arguments(out, json.path(ARGUMENTS));
        } else if (isQueue(kind)) {
            put(out, DURABLE, json.path(DURABLE_FIELD));
            put(out, RabbitMqKinds.AUTO_DELETE, json.path(AUTO_DELETE_FIELD));
            arguments(out, json.path(ARGUMENTS));
            out.remove(RabbitMqKinds.ARGUMENT_PREFIX + QUEUE_TYPE_ARGUMENT);
        } else {
            policyAttributes(kind, json, out);
        }
        return out;
    }

    /** {@link #attributes} for the bindings, the policies, the permissions and the shovels. */
    private static void policyAttributes(KindId kind, JsonNode json, Map<String, Object> out) {
        if (kind.equals(RabbitMqKinds.BINDING) || kind.equals(RabbitMqKinds.EXCHANGE_BINDING)) {
            put(out, FILTER, json.path("routing_key"));
            arguments(out, json.path(ARGUMENTS));
        } else if (kind.equals(RabbitMqKinds.POLICY) || kind.equals(RabbitMqKinds.OPERATOR_POLICY)) {
            put(out, RabbitMqKinds.PATTERN, json.path("pattern"));
            put(out, RabbitMqKinds.APPLY_TO, json.path("apply-to"));
            put(out, RabbitMqKinds.PRIORITY, json.path("priority"));
            for (Map.Entry<String, JsonNode> e : json.path("definition").properties()) {
                out.put(RabbitMqKinds.DEFINITION_PREFIX + e.getKey(), scalar(e.getValue()));
            }
        } else if (kind.equals(RabbitMqKinds.PERMISSION)) {
            put(out, RabbitMqKinds.CONFIGURE, json.path("configure"));
            put(out, RabbitMqKinds.WRITE, json.path(WRITE_FIELD));
            put(out, RabbitMqKinds.READ, json.path("read"));
        } else if (kind.equals(RabbitMqKinds.TOPIC_PERMISSION)) {
            put(out, RabbitMqKinds.WRITE, json.path(WRITE_FIELD));
            put(out, RabbitMqKinds.READ, json.path("read"));
        } else if (kind.equals(RabbitMqKinds.SHOVEL)) {
            out.put(SHOVEL_DEFINITION, json.path("value").toString());
        }
    }

    private static void put(Map<String, Object> out, String key, JsonNode value) {
        if (!value.isMissingNode() && !value.isNull()) {
            out.put(key, scalar(value));
        }
    }

    private static Object scalar(JsonNode value) {
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isIntegralNumber()) {
            return value.asLong();
        }
        return value.isString() ? value.asString() : value.toString();
    }

    private static String tags(JsonNode tags) {
        if (tags.isArray()) {
            List<String> all = new ArrayList<>();
            tags.forEach(t -> all.add(t.asString()));
            return String.join(",", all);
        }
        return tags.asString("");
    }

    private static void arguments(Map<String, Object> out, JsonNode arguments) {
        for (Map.Entry<String, JsonNode> e : arguments.properties()) {
            out.put(RabbitMqKinds.ARGUMENT_PREFIX + e.getKey(), scalar(e.getValue()));
        }
    }

    /** The management JSON that creates the object {@code id} names with {@code attributes}. */
    JsonNode body(ResourceId id, Map<String, Object> attributes) {
        KindId kind = id.kind();
        ObjectNode body = cluster.mapper().createObjectNode();
        if (kind.equals(RabbitMqKinds.VHOST)) {
            text(body, "description", attributes, RabbitMqKinds.DESCRIPTION);
            text(body, "tags", attributes, RabbitMqKinds.TAGS);
            text(body, "default_queue_type", attributes, RabbitMqKinds.DEFAULT_QUEUE_TYPE);
        } else if (kind.equals(RabbitMqKinds.EXCHANGE)) {
            String type = string(attributes, RabbitMqKinds.EXCHANGE_TYPE, null);
            if (type == null) {
                throw RabbitMqErrors.invalid("An exchange needs " + RabbitMqKinds.EXCHANGE_TYPE + ".");
            }
            body.put("type", type);
            body.put(DURABLE_FIELD, flag(attributes, DURABLE, true));
            body.put(AUTO_DELETE_FIELD, flag(attributes, RabbitMqKinds.AUTO_DELETE, false));
            body.put("internal", flag(attributes, RabbitMqKinds.INTERNAL, false));
            body.set(ARGUMENTS, argumentsNode(attributes, Map.of()));
        } else if (isQueue(kind)) {
            body.put(DURABLE_FIELD, flag(attributes, DURABLE, true));
            body.put(AUTO_DELETE_FIELD, flag(attributes, RabbitMqKinds.AUTO_DELETE, false));
            ObjectNode arguments = argumentsNode(
                    attributes,
                    Map.of(
                            RabbitMqKinds.DELIVERY_LIMIT, "x-delivery-limit",
                            RabbitMqKinds.MAX_AGE, "x-max-age",
                            RabbitMqKinds.MAX_LENGTH_BYTES, "x-max-length-bytes"));
            arguments.put(QUEUE_TYPE_ARGUMENT, queueType(kind));
            body.set(ARGUMENTS, arguments);
        } else {
            routedBody(id, attributes, body);
        }
        return body;
    }

    /** {@link #body} for the bindings, the policies and the permissions. */
    private void routedBody(ResourceId id, Map<String, Object> attributes, ObjectNode body) {
        KindId kind = id.kind();
        if (kind.equals(RabbitMqKinds.BINDING) || kind.equals(RabbitMqKinds.EXCHANGE_BINDING)) {
            body.put("routing_key", string(attributes, FILTER, ""));
            body.set(ARGUMENTS, argumentsNode(attributes, Map.of()));
        } else if (kind.equals(RabbitMqKinds.POLICY) || kind.equals(RabbitMqKinds.OPERATOR_POLICY)) {
            String pattern = string(attributes, RabbitMqKinds.PATTERN, null);
            if (pattern == null) {
                throw RabbitMqErrors.invalid("A policy needs " + RabbitMqKinds.PATTERN + ".");
            }
            body.put("pattern", pattern);
            body.put("apply-to", string(attributes, RabbitMqKinds.APPLY_TO, "all"));
            body.put("priority", number(attributes, RabbitMqKinds.PRIORITY));
            ObjectNode definition = body.putObject("definition");
            attributes.forEach((k, v) -> {
                if (k.startsWith(RabbitMqKinds.DEFINITION_PREFIX)) {
                    definition.set(k.substring(RabbitMqKinds.DEFINITION_PREFIX.length()), jsonValue(v));
                }
            });
        } else if (kind.equals(RabbitMqKinds.PERMISSION)) {
            body.put("configure", string(attributes, RabbitMqKinds.CONFIGURE, ""));
            body.put(WRITE_FIELD, string(attributes, RabbitMqKinds.WRITE, ""));
            body.put("read", string(attributes, RabbitMqKinds.READ, ""));
        } else if (kind.equals(RabbitMqKinds.TOPIC_PERMISSION)) {
            body.put(
                    TOPIC_FIELD, ManagementPaths.serverName(id.path().segments().get(2)));
            body.put(WRITE_FIELD, string(attributes, RabbitMqKinds.WRITE, ""));
            body.put("read", string(attributes, RabbitMqKinds.READ, ""));
        } else {
            throw RabbitMqErrors.invalid(kind + " is not created through the management API by this driver.");
        }
    }

    private ObjectNode argumentsNode(Map<String, Object> attributes, Map<String, String> renamed) {
        ObjectNode out = cluster.mapper().createObjectNode();
        attributes.forEach((k, v) -> {
            if (k.startsWith(RabbitMqKinds.ARGUMENT_PREFIX)) {
                out.set(
                        k.substring(RabbitMqKinds.ARGUMENT_PREFIX.length()),
                        cluster.mapper().valueToTree(v));
            } else if (renamed.containsKey(k)) {
                out.set(renamed.get(k), cluster.mapper().valueToTree(v));
            }
        });
        return out;
    }

    private JsonNode jsonValue(Object value) {
        if (value instanceof String text) {
            try {
                return cluster.mapper().readTree(text);
            } catch (JacksonException _) {
                return cluster.mapper().valueToTree(text);
            }
        }
        return cluster.mapper().valueToTree(value);
    }

    private static void text(ObjectNode body, String field, Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value != null) {
            body.put(field, value.toString());
        }
    }

    private static String string(Map<String, Object> attributes, String key, String fallback) {
        Object value = attributes.get(key);
        return value == null ? fallback : value.toString();
    }

    private static boolean flag(Map<String, Object> attributes, String key, boolean fallback) {
        Object value = attributes.get(key);
        return value == null ? fallback : Boolean.parseBoolean(value.toString());
    }

    private static long number(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        try {
            return value == null ? 0 : Long.parseLong(value.toString());
        } catch (NumberFormatException _) {
            throw RabbitMqErrors.invalid(key + " is a whole number, not " + value + ".");
        }
    }

    // Etags.

    /**
     * The etag of {@code current} (empty when the object does not exist): everything that makes the object this one.
     * Never volume, the leader or the policy a queue is under: a failover or a policy edit is not a recreation.
     */
    static String etag(ResourceId id, Optional<JsonNode> current) {
        if (current.isEmpty()) {
            return Etags.of(id.format(), ABSENT);
        }
        JsonNode json = current.get();
        KindId kind = id.kind();
        if (kind.equals(RabbitMqKinds.CONNECTION)) {
            return Etags.of(
                    id.format(),
                    json.path("name").asString(),
                    json.path("connected_at").asString());
        }
        Map<String, Object> attributes = attributes(kind, json);
        if (isQueue(kind)) {
            return Etags.of(
                    id.format(),
                    attributes,
                    json.path("type").asString(queueType(kind)),
                    json.path("exclusive").asBoolean(false),
                    members(json));
        }
        return Etags.of(id.format(), attributes);
    }

    private static List<String> members(JsonNode json) {
        TreeSet<String> all = new TreeSet<>();
        for (String field : List.of("members", "slave_nodes")) {
            json.path(field).forEach(n -> all.add(n.asString()));
        }
        if (json.path("node").isString()) {
            all.add(json.path("node").asString());
        }
        return List.copyOf(all);
    }

    /** A refusal that sent nothing. */
    static BrokerException refused(Status status, String reason, String message) {
        return new BrokerException(status, ErrorInfo.of(reason), Dispatch.NOT_SENT, null, message);
    }
}
