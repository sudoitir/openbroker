package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

/**
 * What the cluster is and offers, read once per connection from {@code /api/overview}, {@code /api/nodes} and
 * {@code /api/whoami}: the version gate and product at once, the plugins enabled on every running node and the
 * user's tags when first asked. A read that the account is not allowed leaves its part unknown (no plugins, no tags)
 * rather than failing the connection.
 */
final class ClusterFeatures {

    static final String PLUGIN_MANAGEMENT = "rabbitmq_management";
    static final String PLUGIN_EVENT_EXCHANGE = "rabbitmq_event_exchange";
    static final String PLUGIN_SHOVEL = "rabbitmq_shovel";
    static final String PLUGIN_SHOVEL_MANAGEMENT = "rabbitmq_shovel_management";
    static final String PLUGIN_FEDERATION = "rabbitmq_federation";
    static final String PLUGIN_FEDERATION_MANAGEMENT = "rabbitmq_federation_management";
    static final String PLUGIN_STREAM = "rabbitmq_stream";

    private static final Pattern NUMBERS = Pattern.compile("^(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?");

    /** A release as three numbers, the text it came from kept for messages. */
    record Version(int major, int minor, int patch, String text) implements Comparable<Version> {
        static Optional<Version> parse(String text) {
            Matcher m = NUMBERS.matcher(text.trim());
            if (!m.find()) {
                return Optional.empty();
            }
            return Optional.of(new Version(
                    Integer.parseInt(m.group(1)),
                    m.group(2) == null ? 0 : Integer.parseInt(m.group(2)),
                    m.group(3) == null ? 0 : Integer.parseInt(m.group(3)),
                    text.trim()));
        }

        @Override
        public int compareTo(Version other) {
            int c = Integer.compare(major, other.major);
            if (c == 0) {
                c = Integer.compare(minor, other.minor);
            }
            return c == 0 ? Integer.compare(patch, other.patch) : c;
        }
    }

    /** What one read found. */
    private record Snapshot(Version version, String product, String productVersion, String clusterName) {}

    private final RabbitMqCluster cluster;
    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();
    private final AtomicReference<Set<String>> pluginsRead = new AtomicReference<>();
    private final AtomicReference<Set<String>> tagsRead = new AtomicReference<>();

    ClusterFeatures(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /** Fails {@code FAILED_PRECONDITION} when the version is outside {@code supportedVersions}; returns this otherwise. */
    ClusterFeatures require(String supportedVersions) {
        Version version = load().version();
        if (!inRange(supportedVersions, version)) {
            throw RabbitMqErrors.unsupportedVersion("This cluster runs RabbitMQ " + version.text()
                    + "; the RabbitMQ driver supports " + supportedVersions + ".");
        }
        return this;
    }

    /** The release the cluster reports. */
    Version version() {
        return load().version();
    }

    /** {@code product_name}, such as {@code RabbitMQ}. */
    String product() {
        return load().product();
    }

    /** {@code product_version}. */
    String productVersion() {
        return load().productVersion();
    }

    String clusterName() {
        return load().clusterName();
    }

    /** The plugins enabled on every node; empty when the account may not read the nodes. */
    Set<String> plugins() {
        Set<String> known = pluginsRead.get();
        if (known == null) {
            synchronized (this) {
                known = pluginsRead.get();
                if (known == null) {
                    known = readPlugins(cluster.management(), Deadlines.current(cluster.options()))
                            .orElse(Set.of());
                    pluginsRead.set(known);
                }
            }
        }
        return known;
    }

    boolean hasPlugin(String plugin) {
        return plugins().contains(plugin);
    }

    boolean eventExchange() {
        return hasPlugin(PLUGIN_EVENT_EXCHANGE);
    }

    boolean shovels() {
        return hasPlugin(PLUGIN_SHOVEL) && hasPlugin(PLUGIN_SHOVEL_MANAGEMENT);
    }

    boolean federation() {
        return hasPlugin(PLUGIN_FEDERATION) && hasPlugin(PLUGIN_FEDERATION_MANAGEMENT);
    }

    boolean streamPlugin() {
        return hasPlugin(PLUGIN_STREAM);
    }

    /** The account's tags, such as {@code administrator}. */
    Set<String> tags() {
        Set<String> known = tagsRead.get();
        if (known == null) {
            synchronized (this) {
                known = tagsRead.get();
                if (known == null) {
                    known = readTags(cluster.management(), Deadlines.current(cluster.options()));
                    tagsRead.set(known);
                }
            }
        }
        return known;
    }

    boolean atLeast(int major, int minor) {
        Version v = version();
        return v.major() > major || (v.major() == major && v.minor() >= minor);
    }

    /** Forgets what was read, so the next question reads again. */
    void reset() {
        synchronized (this) {
            snapshot.set(null);
            pluginsRead.set(null);
            tagsRead.set(null);
        }
    }

    private Snapshot load() {
        Snapshot known = snapshot.get();
        if (known != null) {
            return known;
        }
        synchronized (this) {
            known = snapshot.get();
            if (known == null) {
                known = read();
                snapshot.set(known);
            }
            return known;
        }
    }

    private Snapshot read() {
        Deadline deadline = Deadlines.current(cluster.options());
        ManagementClient client = cluster.management();
        JsonNode overview = overview(client, deadline);
        String text = text(overview, "rabbitmq_version");
        if (text == null) {
            throw RabbitMqErrors.unexpectedSystem(
                    "The management API answered, but not as RabbitMQ does: /api/overview names no rabbitmq_version.");
        }
        Version version = Version.parse(text)
                .orElseThrow(() -> RabbitMqErrors.unexpectedSystem(
                        "RabbitMQ reported a version this driver cannot read: '" + text + "'."));
        return new Snapshot(
                version,
                orEmpty(text(overview, "product_name")),
                orEmpty(text(overview, "product_version")),
                orEmpty(text(overview, "cluster_name")));
    }

    private static JsonNode overview(ManagementClient client, Deadline deadline) {
        JsonNode body;
        try {
            body = client.get("/api/overview", Map.of(), deadline, "read the overview")
                    .body();
        } catch (BrokerException e) {
            if (ErrorReasons.BAD_RESPONSE.equals(e.reason())
                    && "200".equals(e.info().metadata().get(RabbitMqErrors.HTTP_STATUS))) {
                throw RabbitMqErrors.unexpectedSystem(
                        "The management endpoint answered 200, but not with JSON: is it RabbitMQ's?");
            }
            throw e;
        }
        if (!body.isObject()) {
            throw RabbitMqErrors.unexpectedSystem("The management endpoint answered, but not with JSON of RabbitMQ's.");
        }
        return body;
    }

    private static Optional<Set<String>> readPlugins(ManagementClient client, Deadline deadline) {
        try {
            JsonNode nodes = client.get("/api/nodes", Map.of(), deadline, "read the nodes")
                    .body();
            Set<String> common = null;
            if (!nodes.isArray()) {
                return Optional.empty();
            }
            for (JsonNode node : nodes) {
                if (!node.has("enabled_plugins")) {
                    continue; // a node that is down reports no plugins
                }
                Set<String> enabled = new HashSet<>();
                node.path("enabled_plugins").forEach(p -> enabled.add(p.asString()));
                if (common == null) {
                    common = enabled;
                } else {
                    common.retainAll(enabled);
                }
            }
            return Optional.of(common == null ? Set.of() : Set.copyOf(common));
        } catch (BrokerException e) {
            if (e.status() == Status.PERMISSION_DENIED) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private static Set<String> readTags(ManagementClient client, Deadline deadline) {
        try {
            JsonNode tags = client.get("/api/whoami", Map.of(), deadline, "read the account")
                    .body()
                    .path("tags");
            Set<String> out = new LinkedHashSet<>();
            if (tags.isArray()) {
                tags.forEach(t -> out.add(t.asString()));
            } else if (tags.isString()) {
                for (String t : tags.asString().split(",")) {
                    if (!t.isBlank()) {
                        out.add(t.trim());
                    }
                }
            }
            return Set.copyOf(out);
        } catch (BrokerException e) {
            if (e.status() == Status.PERMISSION_DENIED) {
                return Set.of();
            }
            throw e;
        }
    }

    private static boolean inRange(String range, Version version) {
        String text = range.trim();
        List<String> bounds = rangeBounds(text);
        if (bounds.isEmpty()) {
            return Version.parse(range).map(min -> version.compareTo(min) >= 0).orElse(true);
        }
        boolean lowerInclusive = text.charAt(0) == '[';
        boolean upperInclusive = text.charAt(text.length() - 1) == ']';
        Optional<Version> lower = Version.parse(bounds.get(0));
        Optional<Version> upper = Version.parse(bounds.get(1));
        if (lower.isPresent()) {
            int c = version.compareTo(lower.get());
            if (c < 0 || (c == 0 && !lowerInclusive)) {
                return false;
            }
        }
        if (upper.isPresent()) {
            int c = version.compareTo(upper.get());
            return c < 0 || (c == 0 && upperInclusive);
        }
        return true;
    }

    /** The two bounds of an interval such as {@code [3.13.0,4.0)}, or none when {@code text} is not one. */
    private static List<String> rangeBounds(String text) {
        int length = text.length();
        if (length < 3 || "[(".indexOf(text.charAt(0)) < 0 || "])".indexOf(text.charAt(length - 1)) < 0) {
            return List.of();
        }
        int comma = text.indexOf(',');
        if (comma < 0 || text.indexOf(',', comma + 1) >= 0) {
            return List.of();
        }
        String lower = text.substring(1, comma).strip();
        String upper = text.substring(comma + 1, length - 1).strip();
        return isBound(lower) && isBound(upper) ? List.of(lower, upper) : List.of();
    }

    private static boolean isBound(String bound) {
        return bound.chars().noneMatch(c -> Character.isWhitespace(c) || c == ']' || c == ')' || c == '[' || c == '(');
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isString() && !value.asString().isBlank() ? value.asString() : null;
    }

    private static String orEmpty(String value) {
        return value == null ? "" : value;
    }
}
