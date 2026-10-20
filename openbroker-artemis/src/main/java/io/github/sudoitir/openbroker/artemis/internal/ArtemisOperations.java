package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.model.Verb;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Direction;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Changes to queues, addresses and client connections. A queue or address exists per node, so an
 * operation runs on every node of its scope and reports each node's outcome. Preconditions are
 * checked before: every node is read, the etag compared, and only then is anything changed.
 *
 * <p>The etag covers which nodes hold the resource and each one's incarnation (a queue's id, address
 * and routing type), never its volume: a queue recreated since the preview refuses the run, a backlog
 * that grew does not.
 */
public final class ArtemisOperations implements Operations {

    private static final String MESSAGE = "message";
    private static final String RESOURCE = "resource";

    private final Cluster cluster;

    public ArtemisOperations(Cluster cluster) {
        this.cluster = cluster;
    }

    /** What one node holds of the resource: absent, or present with its MBean and attributes. */
    record NodeState(
            NodeHandle node,
            @Nullable String mbean,
            @Nullable JsonNode attributes) {

        boolean present() {
            return attributes != null;
        }

        long num(String attribute) {
            JsonNode v = attributes == null ? null : attributes.get(attribute);
            return v == null || v.isNull() ? 0L : v.asLong();
        }

        @Nullable String text(String attribute) {
            JsonNode v = attributes == null ? null : attributes.get(attribute);
            return v == null || v.isNull() ? null : v.asString();
        }
    }

    @Override
    public Preview preview(ResourceId resource, Operation operation, Scope scope) {
        requireSupported(resource.kind(), operation);
        Deadline deadline = Deadlines.current(cluster.options());
        if (!stateful(resource.kind())) {
            return new Preview(
                    List.of(),
                    Set.of(),
                    new Estimate(Count.exact(1), RESOURCE),
                    etag(resource, List.of()),
                    PreconditionEnforcement.CHECKED_BEFORE);
        }
        List<NodeState> states = states(resource, scope, deadline);
        List<NodeState> present = states.stream().filter(NodeState::present).toList();
        List<AttributeChange> diff = new ArrayList<>();
        Set<String> replace = new HashSet<>();
        Estimate estimate =
                switch (operation) {
                    case Operation.Delete d -> volume(resource.kind(), present);
                    case Operation.Purge p -> volume(resource.kind(), present);
                    case Operation.Create c -> {
                        diff.addAll(changes(resource, Map.of(), c.attributes(), true));
                        yield new Estimate(Count.exact(states.size() - present.size()), RESOURCE);
                    }
                    case Operation.Update u -> {
                        Map<String, Object> before =
                                present.isEmpty() ? Map.of() : config(resource, present.getFirst());
                        diff.addAll(changes(resource, before, u.patch(), false));
                        diff.stream()
                                .map(AttributeChange::key)
                                .filter(QueueConfigs.IMMUTABLE::contains)
                                .forEach(replace::add);
                        yield new Estimate(Count.exact(present.size()), RESOURCE);
                    }
                    case Operation.Close c ->
                        new Estimate(Count.exact(consumersOf(resource, present, deadline)), "consumer");
                    default -> new Estimate(Count.exact(present.size()), RESOURCE);
                };
        return new Preview(diff, replace, estimate, etag(resource, states), PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public Partial<OperationResult> execute(
            ResourceId resource, Operation operation, Scope scope, Precondition precondition) {
        requireSupported(resource.kind(), operation);
        Deadline deadline = Deadlines.current(cluster.options());
        List<NodeHandle> nodes = cluster.nodes(scope);
        if (precondition instanceof Precondition.IfMatch(String expected)) {
            List<NodeState> states = stateful(resource.kind()) ? states(resource, scope, deadline) : List.of();
            if (!etag(resource, states).equals(expected)) {
                return refusedEverywhere(
                        nodes,
                        resource + " changed since it was previewed: it was recreated, or a node joined or left.");
            }
        }
        Partial<List<OperationResult>> results =
                Fanout.each(nodes, cluster.options(), node -> List.of(onNode(resource, operation, node, deadline)));
        return results.map(ArtemisOperations::combine);
    }

    private OperationResult onNode(ResourceId resource, Operation operation, NodeHandle node, Deadline deadline) {
        KindId kind = resource.kind();
        if (kind.equals(ArtemisKinds.QUEUE)) {
            return onQueue(resource, operation, state(resource, node, deadline), deadline);
        }
        if (kind.equals(ArtemisKinds.ADDRESS)) {
            return onAddress(resource, operation, state(resource, node, deadline), deadline);
        }
        if (kind.equals(ArtemisKinds.DIVERT)) {
            return Diverts.on(cluster, resource, operation, node, deadline);
        }
        return close(resource, node, deadline);
    }

    private OperationResult onQueue(ResourceId queue, Operation operation, NodeState state, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(state.node());
        String broker = client.brokerName(deadline);
        String name = queue.name();
        return switch (operation) {
            case Operation.Create c -> {
                Map<String, Object> config = QueueConfigs.toConfig(name, c.attributes(), true);
                JolokiaResponse res = call(
                        client,
                        JolokiaRequest.exec(
                                broker,
                                "createQueue(java.lang.String,boolean)",
                                client.mapper().writeValueAsString(config),
                                false),
                        queue,
                        deadline);
                if (res.ok()) {
                    yield OperationResult.done();
                }
                BrokerException refused = Errors.refusal(state.node().id(), res, "createQueue");
                if (!ErrorReasons.RESOURCE_EXISTS.equals(refused.reason())) {
                    throw refused;
                }
                NodeState existing = state(queue, state.node(), deadline);
                List<String> differing = QueueConfigs.differences(
                        config, QueueConfigs.fromMBean(existing.attributes(), client.mapper()));
                if (!differing.isEmpty()) {
                    throw new BrokerException(
                            Status.ALREADY_EXISTS,
                            ErrorInfo.of(
                                    ErrorReasons.RESOURCE_EXISTS, Map.of("differing", String.join(",", differing))),
                            Dispatch.REJECTED,
                            state.node().id(),
                            "Queue " + name + " exists with a different " + String.join(", ", differing) + ".");
                }
                yield OperationResult.already();
            }
            case Operation.Update u -> {
                requirePresent(queue, state);
                Map<String, Object> current = QueueConfigs.fromMBean(state.attributes(), client.mapper());
                Map<String, Object> patch = QueueConfigs.toConfig(name, u.patch(), false);
                List<String> immutable = QueueConfigs.differences(patch, current).stream()
                        .filter(QueueConfigs.IMMUTABLE::contains)
                        .toList();
                if (!immutable.isEmpty()) {
                    throw new BrokerException(
                            Status.INVALID_ARGUMENT,
                            ErrorInfo.of(ErrorReasons.IMMUTABLE_FIELD, Map.of("fields", String.join(",", immutable))),
                            Dispatch.NOT_SENT,
                            state.node().id(),
                            "A live queue's " + String.join(", ", immutable) + " cannot change; recreate it.");
                }
                // updateQueue replaces the whole configuration, so the current one is sent with the patch over it.
                Map<String, Object> merged = new LinkedHashMap<>(current);
                merged.putAll(patch);
                require(
                        call(
                                client,
                                JolokiaRequest.exec(
                                        broker,
                                        "updateQueue(java.lang.String)",
                                        client.mapper().writeValueAsString(merged)),
                                queue,
                                deadline),
                        state,
                        "updateQueue");
                yield OperationResult.done();
            }
            case Operation.Delete d -> {
                if (!state.present()) {
                    yield OperationResult.already();
                }
                if (d.ifEmpty() && state.num("MessageCount") > 0) {
                    throw new BrokerException(
                            Status.FAILED_PRECONDITION,
                            ErrorInfo.of(ErrorReasons.NOT_EMPTY),
                            Dispatch.NOT_SENT,
                            state.node().id(),
                            "Queue " + name + " still holds " + state.num("MessageCount") + " messages.");
                }
                // The address stays: deleting a queue never takes its address with it.
                JolokiaResponse res = call(
                        client,
                        JolokiaRequest.exec(
                                broker,
                                "destroyQueue(java.lang.String,boolean,boolean)",
                                name,
                                d.disconnectConsumers(),
                                false),
                        queue,
                        deadline);
                cluster.forgetQueue(state.node(), name);
                yield goneIsAlready(res, state, "destroyQueue");
            }
            case Operation.Pause p -> {
                requireDirection(p.direction(), Direction.DELIVERY, "A queue pauses delivery");
                requirePresent(queue, state);
                if (paused(state)) {
                    yield OperationResult.already();
                }
                // Persistently, so the pause survives a broker restart.
                require(
                        call(client, JolokiaRequest.exec(state.mbean(), "pause(boolean)", true), queue, deadline),
                        state,
                        "pause");
                yield OperationResult.done();
            }
            case Operation.Resume r -> {
                requireDirection(r.direction(), Direction.DELIVERY, "A queue resumes delivery");
                requirePresent(queue, state);
                if (!paused(state)) {
                    yield OperationResult.already();
                }
                require(call(client, JolokiaRequest.exec(state.mbean(), "resume()"), queue, deadline), state, "resume");
                yield OperationResult.done();
            }
            case Operation.Purge p -> {
                requirePresent(queue, state);
                JolokiaResponse res =
                        call(client, JolokiaRequest.exec(state.mbean(), "removeAllMessages()"), queue, deadline);
                require(res, state, "removeAllMessages");
                yield OperationResult.affected(
                        res.value() == null ? 0L : res.value().asLong());
            }
            default -> throw Unimplemented.of(operation.getClass().getSimpleName() + " on an Artemis queue");
        };
    }

    private OperationResult onAddress(ResourceId address, Operation operation, NodeState state, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(state.node());
        String broker = client.brokerName(deadline);
        String name = address.name();
        return switch (operation) {
            case Operation.Create c -> {
                String types = routingTypes(c.attributes());
                JolokiaResponse res = call(
                        client,
                        JolokiaRequest.exec(broker, "createAddress(java.lang.String,java.lang.String)", name, types),
                        address,
                        deadline);
                if (res.ok()) {
                    yield OperationResult.done();
                }
                BrokerException refused = Errors.refusal(state.node().id(), res, "createAddress");
                if (!ErrorReasons.RESOURCE_EXISTS.equals(refused.reason())) {
                    throw refused;
                }
                yield OperationResult.already();
            }
            case Operation.Update u -> {
                requirePresent(address, state);
                require(
                        call(
                                client,
                                JolokiaRequest.exec(
                                        broker,
                                        "updateAddress(java.lang.String,java.lang.String)",
                                        name,
                                        routingTypes(u.patch())),
                                address,
                                deadline),
                        state,
                        "updateAddress");
                yield OperationResult.done();
            }
            case Operation.Delete d -> {
                if (!state.present()) {
                    yield OperationResult.already();
                }
                // Force-free: the broker refuses while queues are bound, and that refusal is the answer.
                yield goneIsAlready(
                        call(
                                client,
                                JolokiaRequest.exec(broker, "deleteAddress(java.lang.String)", name),
                                address,
                                deadline),
                        state,
                        "deleteAddress");
            }
            case Operation.Pause p -> {
                requireDirection(p.direction(), Direction.PUBLISH, "An address pauses what producers send");
                requirePresent(address, state);
                if (paused(state)) {
                    yield OperationResult.already();
                }
                require(
                        call(client, JolokiaRequest.exec(state.mbean(), "pause(boolean)", true), address, deadline),
                        state,
                        "pause");
                yield OperationResult.done();
            }
            case Operation.Resume r -> {
                requireDirection(r.direction(), Direction.PUBLISH, "An address resumes what producers send");
                requirePresent(address, state);
                if (!paused(state)) {
                    yield OperationResult.already();
                }
                require(
                        call(client, JolokiaRequest.exec(state.mbean(), "resume()"), address, deadline),
                        state,
                        "resume");
                yield OperationResult.done();
            }
            case Operation.Close c -> {
                JolokiaResponse res = call(
                        client,
                        JolokiaRequest.exec(broker, "closeConsumerConnectionsForAddress(java.lang.String)", name),
                        address,
                        deadline);
                require(res, state, "closeConsumerConnectionsForAddress");
                yield res.value() != null && res.value().asBoolean()
                        ? OperationResult.done()
                        : OperationResult.already();
            }
            default -> throw Unimplemented.of(operation.getClass().getSimpleName() + " on an Artemis address");
        };
    }

    /** Closes a connection, a session or a consumer on the one node that holds it. */
    private OperationResult close(ResourceId resource, NodeHandle node, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        String broker = client.brokerName(deadline);
        List<String> path = resource.path().segments();
        KindId kind = resource.kind();
        JolokiaRequest request;
        if (kind.equals(ArtemisKinds.CONNECTION)) {
            request = JolokiaRequest.exec(broker, "closeConnectionWithID(java.lang.String)", path.getLast());
        } else if (kind.equals(ArtemisKinds.SESSION)) {
            // The broker closes a session by its connection's id too, which only a session row names.
            String connection = connectionOfSession(node, path.getLast(), deadline);
            if (connection == null) {
                return OperationResult.already();
            }
            request = JolokiaRequest.exec(
                    broker, "closeSessionWithID(java.lang.String,java.lang.String)", connection, path.getLast());
        } else if (kind.equals(ArtemisKinds.CONSUMER) && path.size() == 2) {
            request = JolokiaRequest.exec(
                    broker, "closeConsumerWithID(java.lang.String,java.lang.String)", path.get(0), path.get(1));
        } else {
            throw Queries.invalid("Not a closable Artemis resource: " + resource + ".");
        }
        JolokiaResponse res = call(client, request, resource, deadline);
        if (!res.ok()) {
            throw Errors.refusal(node.id(), res, "close");
        }
        // The broker answers false when nothing by that id was open on this node.
        return res.value() != null && res.value().asBoolean() ? OperationResult.done() : OperationResult.already();
    }

    /** A precondition that failed: every node refused, nothing sent. */
    private static Partial<OperationResult> refusedEverywhere(List<NodeHandle> nodes, String message) {
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (NodeHandle node : nodes) {
            outcomes.put(node.id(), NodeOutcome.FAILED);
            failures.put(
                    node.id(),
                    new BrokerException(
                            Status.FAILED_PRECONDITION,
                            ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                            Dispatch.NOT_SENT,
                            node.id(),
                            message));
        }
        return new Partial<>(OperationResult.done(), outcomes, failures);
    }

    /** The connection a session belongs to, or null when the node no longer has the session. */
    private @Nullable String connectionOfSession(NodeHandle node, String session, Deadline deadline) {
        ArtemisLists.Page page = ArtemisLists.fetch(
                cluster.jolokia(node, deadline),
                "listSessions",
                Filters.options(
                        ArtemisKinds.SESSION,
                        ResourceQuery.of(ArtemisKinds.SESSION).named(session)),
                ArtemisLists.FIRST_PAGE,
                ArtemisLists.EVERY_ROW);
        JsonNode rows = page.data();
        if (rows == null || !rows.isArray() || rows.isEmpty()) {
            return null;
        }
        return ArtemisLists.str(rows.get(0), "connectionID");
    }

    // ---- state ------------------------------------------------------------

    private List<NodeState> states(ResourceId resource, Scope scope, Deadline deadline) {
        Partial<List<NodeState>> read =
                Fanout.each(cluster.nodes(scope), cluster.options(), node -> List.of(state(resource, node, deadline)));
        if (!read.failures().isEmpty()) {
            BrokerException first = read.failures().values().iterator().next();
            throw first;
        }
        return read.value();
    }

    private NodeState state(ResourceId resource, NodeHandle node, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        String broker = client.brokerName(deadline);
        String mbean;
        if (resource.kind().equals(ArtemisKinds.QUEUE)) {
            List<String> found = client.search(ArtemisMBeans.queuePattern(broker, resource.name()), deadline);
            if (found.isEmpty()) {
                return new NodeState(node, null, null);
            }
            mbean = found.getFirst();
        } else {
            mbean = ArtemisMBeans.address(broker, resource.name());
        }
        JolokiaResponse res = call(client, JolokiaRequest.readAll(mbean), resource, deadline);
        if (!res.ok()) {
            if (res.errorType() != null && res.errorType().contains(Errors.MBEAN_ABSENT)) {
                return new NodeState(node, null, null);
            }
            throw Errors.refusal(node.id(), res, "read " + resource);
        }
        return new NodeState(node, mbean, res.value());
    }

    private Map<String, Object> config(ResourceId resource, NodeState state) {
        if (resource.kind().equals(ArtemisKinds.QUEUE)) {
            return QueueConfigs.fromMBean(state.attributes(), cluster.mapper());
        }
        List<String> types = new ArrayList<>();
        JsonNode v = state.attributes() == null ? null : state.attributes().get("RoutingTypes");
        if (v != null && v.isArray()) {
            v.forEach(t -> types.add(t.asString()));
        }
        return Map.of(ArtemisKinds.ROUTING_TYPES, String.join(",", types));
    }

    /** The resource's identity over the nodes read: which hold it, and each one's incarnation. */
    static String etag(ResourceId resource, List<NodeState> states) {
        List<String> lines = new ArrayList<>();
        lines.add(resource.toString());
        for (NodeState s : states) {
            lines.add(s.node().id().value() + "|"
                    + (s.present()
                            ? Objects.toString(s.text("ID"), Objects.toString(s.text("Id"), ""))
                                    + "|" + Objects.toString(s.text("Address"), "")
                                    + "|" + Objects.toString(s.text("RoutingType"), "")
                            : "absent"));
        }
        lines.subList(1, lines.size()).sort(null);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
            return "W/\"" + Base64.getUrlEncoder().withoutPadding().encodeToString(digest) + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }

    /** The consumers attached to an address on the nodes that hold it, as the broker's own list filter counts them. */
    private long consumersOf(ResourceId address, List<NodeState> present, Deadline deadline) {
        long total = 0;
        for (NodeState state : present) {
            total += ArtemisLists.fetch(
                            cluster.jolokia(state.node(), deadline),
                            "listConsumers",
                            Filters.options(
                                    ArtemisKinds.CONSUMER,
                                    ResourceQuery.of(ArtemisKinds.CONSUMER)
                                            .where(Map.of(StandardAttributes.DESTINATION.name(), address.toString()))),
                            ArtemisLists.FIRST_PAGE,
                            ArtemisLists.EVERY_ROW)
                    .count();
        }
        return total;
    }

    // ---- helpers ----------------------------------------------------------

    private static boolean stateful(KindId kind) {
        return kind.equals(ArtemisKinds.QUEUE) || kind.equals(ArtemisKinds.ADDRESS);
    }

    private static void requireSupported(KindId kind, Operation operation) {
        Verb verb =
                switch (operation) {
                    case Operation.Create c -> Verb.CREATE;
                    case Operation.Update u -> Verb.UPDATE;
                    case Operation.Delete d -> Verb.DELETE;
                    case Operation.Pause p -> Verb.PAUSE;
                    case Operation.Resume r -> Verb.RESUME;
                    case Operation.Purge p -> Verb.PURGE;
                    case Operation.Close c -> Verb.CLOSE;
                    default -> null;
                };
        if (verb == null || !Catalog.supports(kind, verb)) {
            throw Unimplemented.of(operation.getClass().getSimpleName() + " on " + kind);
        }
    }

    private static Estimate volume(KindId kind, List<NodeState> present) {
        String attribute = kind.equals(ArtemisKinds.QUEUE) ? "MessageCount" : "NumberOfMessages";
        return new Estimate(
                Count.exact(present.stream().mapToLong(s -> s.num(attribute)).sum()), MESSAGE);
    }

    private static List<AttributeChange> changes(
            ResourceId resource, Map<String, Object> before, Attributes after, boolean create) {
        Map<String, Object> next = resource.kind().equals(ArtemisKinds.QUEUE)
                ? QueueConfigs.toConfig(resource.name(), after, create)
                : Map.of(ArtemisKinds.ROUTING_TYPES, routingTypes(after));
        List<AttributeChange> out = new ArrayList<>();
        next.forEach((key, value) -> {
            Object old = before.get(key);
            if (!Objects.equals(String.valueOf(old), String.valueOf(value))) {
                out.add(new AttributeChange(key, Optional.ofNullable(old), Optional.of(value)));
            }
        });
        return out;
    }

    private static String routingTypes(Attributes attributes) {
        return attributes
                .raw(ArtemisKinds.ROUTING_TYPES)
                .or(() -> attributes.raw(ArtemisKinds.ROUTING_TYPE))
                .map(String::valueOf)
                .orElse("ANYCAST");
    }

    private static boolean paused(NodeState state) {
        JsonNode v = state.attributes() == null ? null : state.attributes().get("Paused");
        return v != null && v.asBoolean();
    }

    private static void requireDirection(Direction asked, Direction supported, String what) {
        if (asked != supported) {
            throw Queries.invalid(what + " only; " + asked + " is not supported.");
        }
    }

    private static void requirePresent(ResourceId resource, NodeState state) {
        if (!state.present()) {
            throw new BrokerException(
                    Status.NOT_FOUND,
                    ErrorInfo.of(ErrorReasons.RESOURCE_GONE),
                    Dispatch.NOT_SENT,
                    state.node().id(),
                    resource + " is not on this node.");
        }
    }

    private static JolokiaResponse call(
            JolokiaClient client, JolokiaRequest request, ResourceId target, Deadline deadline) {
        return client.single(request, deadline, Optional.of(target));
    }

    private static void require(JolokiaResponse res, NodeState state, String operation) {
        if (!res.ok()) {
            throw Errors.refusal(state.node().id(), res, operation);
        }
    }

    private static OperationResult goneIsAlready(JolokiaResponse res, NodeState state, String operation) {
        if (res.ok()) {
            return OperationResult.done();
        }
        BrokerException refused = Errors.refusal(state.node().id(), res, operation);
        if (ErrorReasons.RESOURCE_GONE.equals(refused.reason())) {
            return OperationResult.already();
        }
        throw refused;
    }

    private static OperationResult combine(List<OperationResult> results) {
        if (results.isEmpty()) {
            return OperationResult.done();
        }
        boolean counted = results.stream().anyMatch(r -> r.affected().isPresent());
        OptionalLong affected = counted
                ? OptionalLong.of(
                        results.stream().mapToLong(r -> r.affected().orElse(0L)).sum())
                : OptionalLong.empty();
        boolean already = results.stream().allMatch(OperationResult::alreadyInState);
        return new OperationResult(affected, Optional.empty(), already);
    }
}
