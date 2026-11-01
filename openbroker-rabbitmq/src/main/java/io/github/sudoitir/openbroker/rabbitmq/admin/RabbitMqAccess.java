package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.access.Remedy;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/**
 * {@link Access}: what the account may do, from its user tags, its permissions and the enabled plugins. It never
 * changes the cluster: one {@code GET /api/whoami} settles the management capabilities, the plugins come from
 * {@link ClusterFeatures}, and a question about a resource's kind is answered from the kind with no request.
 */
final class RabbitMqAccess implements Access {

    /** The event exchange {@code amq.rabbitmq.event} exists, so events can be read from it. */
    static final Capability EVENT_EXCHANGE = new Capability("rabbitmq.event-exchange");

    /** The shovel plugin and its management plugin are enabled on every node. */
    static final Capability SHOVELS = new Capability("rabbitmq.shovels");

    /** The federation plugin and its management plugin are enabled on every node. */
    static final Capability FEDERATION = new Capability("rabbitmq.federation");

    /** The cluster can host streams. */
    static final Capability STREAMS = new Capability("rabbitmq.streams");

    private static final String SHELL = "text/x-shellscript";
    private static final String EVENT_EXCHANGE_NAME = "amq.rabbitmq.event";
    private static final String NOT_TRIED = "Not tried: the first real call settles it.";
    private static final String NOT_ASSESSED = "Not assessed: management reads are not available.";
    private static final String CANNOT_TAKE = "RabbitMQ cannot take one message out of a queue.";
    private static final String STREAMS_ONLY =
            "Streams only: classic and quorum queues cannot be read without taking their messages.";
    private static final String NO_DATA = "The connection has no amqp endpoint.";
    private static final String ADD_DATA = "Add an endpoint on the data plane (an amqp address) to the connection.";
    private static final String ENABLE_MANAGEMENT = "rabbitmq-plugins enable rabbitmq_management";
    private static final String ENABLE_EVENT_EXCHANGE = "rabbitmq-plugins enable rabbitmq_event_exchange";
    private static final String ENABLE_SHOVEL = "rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management";
    private static final String ENABLE_FEDERATION =
            "rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management";
    private static final String ENABLE_STREAMS = "rabbitmqctl enable_feature_flag stream_queue";
    private static final String PLUGINS_UNREAD =
            "The enabled plugins could not be read: the account may not read nodes.";

    /** What {@code /api/whoami} said: the assessment of reading, and the account behind it. */
    private record Read(Assessment assessment, String user, Set<String> tags) {
        boolean available() {
            return assessment.status() == Assessment.Status.AVAILABLE;
        }
    }

    private final RabbitMqCluster cluster;

    RabbitMqAccess(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Assessment check(Capability capability, Optional<ResourceId> resource) {
        if (Capability.BROWSE.equals(capability)) {
            return browse(resource);
        }
        if (Capability.SEND.equals(capability)) {
            return hasData() ? Assessment.unknown(NOT_TRIED) : noData();
        }
        if (Capability.RECEIVE.equals(capability)) {
            return receive(resource);
        }
        if (Capability.MOVE_MESSAGES.equals(capability)
                || Capability.COPY_MESSAGES.equals(capability)
                || Capability.DELETE_MESSAGES.equals(capability)) {
            return Assessment.unavailable(CANNOT_TAKE, Optional.empty());
        }
        Read read = read(Deadlines.current(cluster.options()));
        if (Capability.MANAGEMENT_READ.equals(capability)) {
            return read.assessment();
        }
        if (!read.available()) {
            return Assessment.unknown(NOT_ASSESSED);
        }
        if (Capability.MANAGEMENT_WRITE.equals(capability)) {
            return write(read);
        }
        if (Capability.NOTIFICATIONS.equals(capability)) {
            return notifications();
        }
        if (EVENT_EXCHANGE.equals(capability)) {
            return eventExchange(Deadlines.current(cluster.options()));
        }
        if (SHOVELS.equals(capability)) {
            return plugins(ClusterFeatures.PLUGIN_SHOVEL, ClusterFeatures.PLUGIN_SHOVEL_MANAGEMENT, ENABLE_SHOVEL);
        }
        if (FEDERATION.equals(capability)) {
            return plugins(
                    ClusterFeatures.PLUGIN_FEDERATION, ClusterFeatures.PLUGIN_FEDERATION_MANAGEMENT, ENABLE_FEDERATION);
        }
        if (STREAMS.equals(capability)) {
            return streams(Deadlines.current(cluster.options()));
        }
        return Assessment.unknown("Not assessed: this driver does not know the capability " + capability.id() + ".");
    }

    @Override
    public AccessSnapshot probe() {
        Deadline deadline = Deadlines.current(cluster.options());
        Read read = read(deadline);
        Map<Capability, Assessment> out = new LinkedHashMap<>();
        out.put(Capability.MANAGEMENT_READ, read.assessment());
        out.put(Capability.BROWSE, browse(Optional.empty()));
        out.put(Capability.SEND, hasData() ? Assessment.unknown(NOT_TRIED) : noData());
        out.put(Capability.RECEIVE, receive(Optional.empty()));
        Assessment cannotTake = Assessment.unavailable(CANNOT_TAKE, Optional.empty());
        out.put(Capability.MOVE_MESSAGES, cannotTake);
        out.put(Capability.COPY_MESSAGES, cannotTake);
        out.put(Capability.DELETE_MESSAGES, cannotTake);
        if (read.available()) {
            out.put(Capability.MANAGEMENT_WRITE, write(read));
            out.put(Capability.NOTIFICATIONS, notifications());
            out.put(EVENT_EXCHANGE, eventExchange(deadline));
            out.put(
                    SHOVELS,
                    plugins(ClusterFeatures.PLUGIN_SHOVEL, ClusterFeatures.PLUGIN_SHOVEL_MANAGEMENT, ENABLE_SHOVEL));
            out.put(
                    FEDERATION,
                    plugins(
                            ClusterFeatures.PLUGIN_FEDERATION,
                            ClusterFeatures.PLUGIN_FEDERATION_MANAGEMENT,
                            ENABLE_FEDERATION));
            out.put(STREAMS, streams(deadline));
        } else {
            Assessment unassessed = Assessment.unknown(NOT_ASSESSED);
            out.put(Capability.MANAGEMENT_WRITE, unassessed);
            out.put(Capability.NOTIFICATIONS, unassessed);
        }
        return new AccessSnapshot(out, cluster.options().clock().instant());
    }

    private Read read(Deadline deadline) {
        try {
            JsonNode body = cluster.management()
                    .get("/api/whoami", Map.of(), deadline, "read the account")
                    .body();
            return new Read(
                    Assessment.available("The account can read the management API."),
                    body.path("name").isString() ? body.path("name").asString() : "<user>",
                    tags(body.path("tags")));
        } catch (BrokerException e) {
            return new Read(unreadable(e), "<user>", Set.of());
        }
    }

    private static Assessment unreadable(BrokerException e) {
        if (e.status() == Status.UNAUTHENTICATED) {
            return Assessment.unavailable(e.getMessage(), Optional.empty());
        }
        if (e.status() == Status.PERMISSION_DENIED) {
            return Assessment.unavailable(
                    e.getMessage(),
                    Optional.of(new Remedy(
                            "Give the account a management tag.",
                            "rabbitmqctl set_user_tags <user> monitoring",
                            SHELL)));
        }
        boolean missing = ErrorReasons.ENDPOINT_NOT_FOUND.equals(e.reason())
                || (ErrorReasons.UNREACHABLE.equals(e.reason()) && e.dispatch() == Dispatch.NOT_SENT);
        if (missing) {
            return Assessment.unavailable(
                    e.getMessage(), Optional.of(new Remedy("Enable the management plugin.", ENABLE_MANAGEMENT, SHELL)));
        }
        return Assessment.unknown(e.getMessage());
    }

    private static Set<String> tags(JsonNode tags) {
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
        return out;
    }

    private static Assessment write(Read read) {
        Set<String> tags = read.tags();
        if (tags.contains("administrator")) {
            return Assessment.available("The account has the administrator tag.");
        }
        if (tags.contains("policymaker") || tags.contains("management")) {
            return Assessment.unknown(
                    "Writes depend on the vhost's configure permission; the first real call settles it.");
        }
        if (tags.isEmpty()) {
            return Assessment.unknown("The account's tags could not be read; the first real call settles it.");
        }
        return Assessment.unavailable(
                "The account's tags (" + String.join(", ", tags) + ") do not allow changing the cluster.",
                Optional.of(new Remedy(
                        "Give the account the policymaker or administrator tag.",
                        "rabbitmqctl set_user_tags " + read.user() + " policymaker",
                        SHELL)));
    }

    private Assessment notifications() {
        String vhost = RabbitMqOptions.eventVhost(cluster.spec());
        String interval = RabbitMqOptions.watchInterval(cluster.spec()).toString();
        String why;
        if (!hasData()) {
            why = "the connection has no amqp endpoint";
        } else {
            try {
                if (cluster.features().eventExchange()) {
                    return Assessment.available("Events from " + EVENT_EXCHANGE_NAME + " in vhost " + vhost);
                }
                why = "the event exchange plugin is not enabled";
            } catch (BrokerException _) {
                why = "the plugins could not be read";
            }
        }
        return Assessment.available("Polled every " + interval + ": " + why);
    }

    private Assessment eventExchange(Deadline deadline) {
        String vhost = RabbitMqOptions.eventVhost(cluster.spec());
        try {
            cluster.management()
                    .get(
                            "/api/exchanges/" + ManagementPaths.segment(vhost) + "/" + EVENT_EXCHANGE_NAME,
                            Map.of(),
                            deadline,
                            "read the event exchange");
            return Assessment.available(EVENT_EXCHANGE_NAME + " exists in vhost " + vhost + ".");
        } catch (BrokerException e) {
            if (e.status() == Status.NOT_FOUND) {
                return Assessment.unavailable(
                        EVENT_EXCHANGE_NAME + " does not exist in vhost " + vhost + ".",
                        Optional.of(new Remedy("Enable the event exchange plugin.", ENABLE_EVENT_EXCHANGE, SHELL)));
            }
            return Assessment.unknown(e.getMessage());
        }
    }

    private Assessment plugins(String plugin, String management, String enable) {
        try {
            Set<String> enabled = cluster.features().plugins();
            if (enabled.isEmpty()) {
                return Assessment.unknown(PLUGINS_UNREAD);
            }
            if (enabled.contains(plugin) && enabled.contains(management)) {
                return Assessment.available(plugin + " and " + management + " are enabled on every node.");
            }
            return Assessment.unavailable(
                    plugin + " and " + management + " are not both enabled on every node.",
                    Optional.of(new Remedy("Enable the plugins on every node.", enable, SHELL)));
        } catch (BrokerException e) {
            return Assessment.unknown(e.getMessage());
        }
    }

    private Assessment streams(Deadline deadline) {
        try {
            if (cluster.features().atLeast(4, 0)) {
                return Assessment.available("Streams are always enabled from RabbitMQ 4.0.");
            }
            JsonNode flags = cluster.management()
                    .get("/api/feature-flags", Map.of(), deadline, "read the feature flags")
                    .body();
            for (JsonNode flag : flags) {
                if ("stream_queue".equals(flag.path("name").asString())) {
                    if ("enabled".equals(flag.path("state").asString())) {
                        return Assessment.available("The stream_queue feature flag is enabled.");
                    }
                    return Assessment.unavailable(
                            "The stream_queue feature flag is not enabled.",
                            Optional.of(new Remedy("Enable the feature flag.", ENABLE_STREAMS, SHELL)));
                }
            }
            return Assessment.unknown("The cluster lists no stream_queue feature flag.");
        } catch (BrokerException e) {
            return Assessment.unknown(e.getMessage());
        }
    }

    private Assessment browse(Optional<ResourceId> resource) {
        if (resource.isEmpty()) {
            return Assessment.available(STREAMS_ONLY);
        }
        KindId kind = resource.get().kind();
        if (RabbitMqKinds.CLASSIC_QUEUE.equals(kind) || RabbitMqKinds.QUORUM_QUEUE.equals(kind)) {
            return Assessment.unavailable(STREAMS_ONLY, Optional.empty());
        }
        if (!hasData()) {
            return noData();
        }
        return Assessment.unknown("Not tried: the first read settles whether the account may read the stream.");
    }

    private Assessment receive(Optional<ResourceId> resource) {
        if (resource.isPresent() && RabbitMqKinds.STREAM.equals(resource.get().kind())) {
            return Assessment.unavailable("A stream keeps its messages; browse it.", Optional.empty());
        }
        return hasData() ? Assessment.unknown(NOT_TRIED) : noData();
    }

    private static Assessment noData() {
        return Assessment.unavailable(NO_DATA, Optional.of(new Remedy(ADD_DATA, "amqp://<host>:5672", "text/plain")));
    }

    private boolean hasData() {
        ClusterSpec spec = cluster.spec();
        for (Endpoint endpoint : spec.clusterEndpoints()) {
            if (endpoint.plane() == Plane.DATA) {
                return true;
            }
        }
        for (NodeSpec node : spec.nodes()) {
            for (Endpoint endpoint : node.endpoints()) {
                if (endpoint.plane() == Plane.DATA) {
                    return true;
                }
            }
        }
        return false;
    }
}
