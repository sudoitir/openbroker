package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ClusterMember;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Serving;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/** {@link Nodes} from {@code /api/nodes} and the listeners of {@code /api/overview}; the node ids are the native node names. */
final class RabbitMqNodes implements Nodes {

    /** Node attribute: {@code product_name} of the overview. */
    static final String PRODUCT_NAME = "rabbitmq.product_name";

    /** Node attribute: {@code product_version} of the overview. */
    static final String PRODUCT_VERSION = "rabbitmq.product_version";

    private static final Map<String, String> PROTOCOLS = Map.of(
            "amqp", RabbitMqOptions.AMQP,
            "amqp/ssl", RabbitMqOptions.AMQPS,
            "http", RabbitMqOptions.HTTP,
            "https", RabbitMqOptions.HTTPS,
            "stream", "stream",
            "stream/ssl", "stream+ssl");

    private static final String REACH_PATH = "/api/health/checks/local-alarms";

    private final RabbitMqCluster cluster;

    RabbitMqNodes(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Partial<List<NodeDescription>> describe(Scope scope) {
        List<NodeDescription> all = read(scope);
        return Partial.complete(all, all.stream().map(NodeDescription::node).toList());
    }

    /**
     * Checks every node the caller named a management endpoint for on that endpoint alone. The cluster describes all
     * its nodes through whichever node answers, so a node the caller cannot reach is still described; it becomes a
     * failed node here, with the failure the call met, and keeps its description.
     */
    Partial<List<NodeDescription>> reach(Partial<List<NodeDescription>> described, Deadline deadline) {
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>(described.outcomes());
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>(described.failures());
        Set<NodeId> named = new LinkedHashSet<>();
        cluster.managementTargets().forEach(t -> named.add(t.node()));
        for (NodeDescription node : described.value()) {
            if (!named.contains(node.node())) {
                continue;
            }
            try {
                cluster.management().getAt(node.node(), REACH_PATH, deadline, "reach the node");
            } catch (BrokerException e) {
                outcomes.put(
                        node.node(), e.dispatch() == Dispatch.IN_DOUBT ? NodeOutcome.IN_DOUBT : NodeOutcome.FAILED);
                failures.put(node.node(), e);
            }
        }
        return new Partial<>(described.value(), outcomes, failures);
    }

    @Override
    public Partial<List<ClusterMember>> members(Scope scope) {
        List<NodeDescription> all = read(scope);
        List<ClusterMember> members = new ArrayList<>();
        for (NodeDescription node : all) {
            List<ClusterMember.Replica> replicas = node.endpoints().stream()
                    .filter(e -> e.plane() == Plane.DATA)
                    .findFirst()
                    .or(() -> node.endpoints().stream().findFirst())
                    .map(e -> List.of(new ClusterMember.Replica(Designation.NONE, e)))
                    .orElse(List.of());
            members.add(new ClusterMember(node.nativeId(), replicas, node.attributes()));
        }
        return Partial.complete(members, all.stream().map(NodeDescription::node).toList());
    }

    private List<NodeDescription> read(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return cluster.call("describe nodes", Optional.empty(), deadline, () -> {
            ManagementClient client = cluster.management();
            JsonNode nodes = client.get("/api/nodes", Map.of(), deadline, "read the nodes")
                    .body();
            JsonNode overview = client.get("/api/overview", Map.of(), deadline, "read the overview")
                    .body();
            if (!nodes.isArray()) {
                throw RabbitMqErrors.badResponse("/api/nodes did not answer with a list.", null);
            }
            Map<String, List<Endpoint>> listeners = listeners(overview.path("listeners"));
            List<NodeDescription> out = new ArrayList<>();
            for (JsonNode node : nodes) {
                String name = node.path("name").asString();
                if (selected(scope, name)) {
                    out.add(describe(node, overview, listeners.getOrDefault(name, List.of())));
                }
            }
            requireAll(scope, out);
            return out;
        });
    }

    private static boolean selected(Scope scope, String name) {
        return switch (scope) {
            case Scope.Whole _ -> true;
            case Scope.Shards s ->
                s.shards().stream()
                        .anyMatch(shard -> shard instanceof Shard.NodeInstance(NodeId node)
                                && node.value().equals(name));
        };
    }

    private static void requireAll(Scope scope, List<NodeDescription> found) {
        if (!(scope instanceof Scope.Shards shards)) {
            return;
        }
        for (Shard shard : shards.shards()) {
            if (!(shard instanceof Shard.NodeInstance(NodeId node))) {
                throw RabbitMqErrors.invalid("RabbitMQ has no partitions; scope work to nodes.");
            }
            if (found.stream().noneMatch(d -> d.node().equals(node))) {
                throw new BrokerException(
                        Status.NOT_FOUND,
                        ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                        Dispatch.NOT_SENT,
                        node,
                        "The cluster has no node " + node.value() + ".");
            }
        }
    }

    private static Map<String, List<Endpoint>> listeners(JsonNode listeners) {
        Map<String, List<Endpoint>> byNode = new LinkedHashMap<>();
        for (JsonNode listener : listeners) {
            String protocol = PROTOCOLS.get(listener.path("protocol").asString());
            String node = listener.path("node").asString();
            if (protocol == null || node.isEmpty()) {
                continue;
            }
            String host = node.contains("@") ? node.substring(node.indexOf('@') + 1) : node;
            boolean management = protocol.startsWith("http");
            String address = management
                    ? protocol + "://" + host + ":" + listener.path("port").asInt()
                    : host + ":" + listener.path("port").asInt();
            byNode.computeIfAbsent(node, k -> new ArrayList<>())
                    .add(new Endpoint(management ? Plane.MANAGEMENT : Plane.DATA, protocol, address));
        }
        return byNode;
    }

    private static NodeDescription describe(JsonNode node, JsonNode overview, List<Endpoint> endpoints) {
        String name = node.path("name").asString();
        boolean running = node.path("running").asBoolean(false);
        boolean memory = node.path("mem_alarm").asBoolean(false);
        boolean disk = node.path("disk_free_alarm").asBoolean(false);
        List<String> partitions = new ArrayList<>();
        node.path("partitions").forEach(p -> partitions.add(p.asString()));
        List<Condition> conditions = conditions(memory, disk, running, partitions);
        Attributes.Builder attributes = Attributes.builder();
        text(overview, "product_name").ifPresent(v -> attributes.putRaw(PRODUCT_NAME, v));
        text(overview, "product_version").ifPresent(v -> attributes.putRaw(PRODUCT_VERSION, v));
        Optional<String> clusterName = text(overview, "cluster_name");
        return new NodeDescription(
                NodeId.of(name),
                name,
                new Membership(
                        clusterName, Designation.NONE, running ? Serving.ACTIVE : Serving.UNKNOWN, Optional.empty()),
                Set.of(NodeFunction.BROKER),
                endpoints,
                version(node),
                conditions,
                attributes.build(),
                Optional.empty());
    }

    private static List<Condition> conditions(boolean memory, boolean disk, boolean running, List<String> partitions) {
        boolean alarm = memory || disk;
        boolean partitioned = !partitions.isEmpty();
        return List.of(
                new Condition(
                        ConditionTypes.FULL,
                        alarm ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                        alarmReason(memory, disk),
                        alarm ? "The node raised a " + (memory ? "memory" : "disk space") + " alarm." : "",
                        Optional.empty()),
                new Condition(
                        ConditionTypes.SERVING,
                        running ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                        running ? "" : "not_running",
                        running ? "" : "The node is not running.",
                        Optional.empty()),
                new Condition(
                        ConditionTypes.OUT_OF_SYNC,
                        partitioned ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                        partitioned ? "network_partition" : "",
                        partitioned ? "The node is partitioned from " + String.join(", ", partitions) + "." : "",
                        Optional.empty()));
    }

    private static String alarmReason(boolean memory, boolean disk) {
        if (memory) {
            return "mem_alarm";
        }
        return disk ? "disk_free_alarm" : "";
    }

    private static Optional<String> version(JsonNode node) {
        for (JsonNode application : node.path("applications")) {
            if ("rabbit".equals(application.path("name").asString())) {
                return text(application, "version");
            }
        }
        return Optional.empty();
    }

    private static Optional<String> text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isString() && !value.asString().isBlank() ? Optional.of(value.asString()) : Optional.empty();
    }
}
