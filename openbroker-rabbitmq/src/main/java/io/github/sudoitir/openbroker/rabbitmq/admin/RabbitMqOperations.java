package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Operations;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/**
 * {@link Operations}: previews and executes the verbs of each kind, with etags that name the state acted on. The
 * management API has no conditional request, so every precondition is checked before the change
 * ({@link PreconditionEnforcement#CHECKED_BEFORE}) against a fresh read, and RabbitMQ exposes no incarnation, so
 * every etag is {@link EtagBasis#CONFIGURATION}: a resource deleted and created again the same is not told apart.
 */
final class RabbitMqOperations implements Operations {

    private static final String MESSAGES = "{message}";
    private static final String MESSAGE_COUNT = "messages";
    private static final Set<KindId> CREATE = Set.of(
            RabbitMqKinds.VHOST,
            RabbitMqKinds.EXCHANGE,
            RabbitMqKinds.CLASSIC_QUEUE,
            RabbitMqKinds.QUORUM_QUEUE,
            RabbitMqKinds.STREAM,
            RabbitMqKinds.BINDING,
            RabbitMqKinds.EXCHANGE_BINDING,
            RabbitMqKinds.POLICY,
            RabbitMqKinds.OPERATOR_POLICY,
            RabbitMqKinds.PERMISSION,
            RabbitMqKinds.TOPIC_PERMISSION);
    private static final Set<KindId> UPDATE = Set.of(
            RabbitMqKinds.VHOST,
            RabbitMqKinds.POLICY,
            RabbitMqKinds.OPERATOR_POLICY,
            RabbitMqKinds.PERMISSION,
            RabbitMqKinds.TOPIC_PERMISSION);
    private static final Set<KindId> PURGE = Set.of(RabbitMqKinds.CLASSIC_QUEUE, RabbitMqKinds.QUORUM_QUEUE);

    private final RabbitMqCluster cluster;
    private final Writes writes;

    RabbitMqOperations(RabbitMqCluster cluster) {
        this.cluster = cluster;
        this.writes = new Writes(cluster);
    }

    /** What an operation acts on, read once: the object, its etag and what the change would do. */
    private record Plan(
            Optional<JsonNode> current, String etag, List<AttributeChange> diff, Estimate estimate, long ready) {}

    @Override
    public Preview preview(ResourceId resource, Operation operation, Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return cluster.call(deadline, () -> {
            Plan plan = plan(resource, operation, deadline);
            return new Preview(
                    plan.diff(),
                    Set.of(),
                    plan.estimate(),
                    plan.etag(),
                    EtagBasis.CONFIGURATION,
                    PreconditionEnforcement.CHECKED_BEFORE);
        });
    }

    @Override
    public Partial<OperationResult> execute(
            ResourceId resource, Operation operation, Scope scope, Precondition precondition) {
        Deadline deadline = Deadlines.current(cluster.options());
        try {
            OperationResult result = cluster.call(deadline, () -> run(resource, operation, precondition, deadline));
            return new Partial<>(result, Map.of(RabbitMqCluster.CLUSTER, NodeOutcome.APPLIED), Map.of());
        } catch (BrokerException e) {
            BrokerException failure = e.at(RabbitMqCluster.CLUSTER);
            return new Partial<>(
                    OperationResult.done(),
                    Map.of(RabbitMqCluster.CLUSTER, outcome(failure)),
                    Map.of(RabbitMqCluster.CLUSTER, failure));
        }
    }

    private static NodeOutcome outcome(BrokerException failure) {
        return switch (failure.dispatch()) {
            case IN_DOUBT -> NodeOutcome.IN_DOUBT;
            case NOT_SENT -> NodeOutcome.SKIPPED;
            default -> NodeOutcome.FAILED;
        };
    }

    private OperationResult run(ResourceId id, Operation operation, Precondition precondition, Deadline deadline) {
        Plan plan = plan(id, operation, deadline);
        if (precondition instanceof Precondition.IfMatch(String etag) && !etag.equals(plan.etag())) {
            throw Writes.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.PRECONDITION_FAILED,
                    id + " changed since the preview; preview it again.");
        }
        switch (operation) {
            case Operation.Create(Attributes attributes) -> writes.put(id, attributes.asMap(), deadline);
            case Operation.Update(Attributes patch) -> {
                Map<String, Object> merged = new LinkedHashMap<>(
                        Writes.attributes(id.kind(), plan.current().orElseThrow()));
                merged.putAll(patch.asMap());
                writes.put(id, merged, deadline);
            }
            case Operation.Delete delete -> delete(id, delete, plan.current().orElseThrow(), deadline);
            case Operation.Purge _ -> {
                writes.purge(id, deadline);
                return OperationResult.affected(plan.ready());
            }
            case Operation.Close _ -> writes.delete(id, Map.of(), deadline);
            default -> throw unsupported(id, operation);
        }
        return OperationResult.done();
    }

    private void delete(ResourceId id, Operation.Delete delete, JsonNode current, Deadline deadline) {
        KindId kind = id.kind();
        Map<String, String> query = Map.of();
        if (kind.equals(RabbitMqKinds.CLASSIC_QUEUE)) {
            query = Map.of(
                    "if-empty", Boolean.toString(delete.ifEmpty()),
                    "if-unused", Boolean.toString(!delete.disconnectConsumers()));
        } else if (Writes.isQueue(kind)) {
            // Quorum queues and streams refuse the conditions as flags, so they are checked here, from the read above.
            if (delete.ifEmpty() && current.path(MESSAGE_COUNT).asLong(0) > 0) {
                throw Writes.refused(Status.FAILED_PRECONDITION, ErrorReasons.NOT_EMPTY, id + " holds messages.");
            }
            if (!delete.disconnectConsumers() && current.path("consumers").asLong(0) > 0) {
                throw Writes.refused(Status.FAILED_PRECONDITION, ErrorReasons.HAS_CONSUMERS, id + " has consumers.");
            }
        } else if (kind.equals(RabbitMqKinds.EXCHANGE)) {
            query = exchangeConditions(id, delete, deadline);
        } else if (kind.equals(RabbitMqKinds.VHOST)
                && delete.ifEmpty()
                && !writes.queues(id.name(), deadline).isEmpty()) {
            throw Writes.refused(Status.FAILED_PRECONDITION, ErrorReasons.HAS_CHILDREN, id + " holds queues.");
        } else if (kind.equals(RabbitMqKinds.TOPIC_PERMISSION) && writes.topicPermissions(id, deadline) > 1) {
            throw Writes.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "RabbitMQ removes all of a user's topic permissions in a virtual host at once, and " + id
                            + " is not the only one.");
        }
        writes.delete(id, query, deadline);
    }

    private Map<String, String> exchangeConditions(ResourceId id, Operation.Delete delete, Deadline deadline) {
        List<String> path = id.path().segments();
        if (path.get(1).startsWith("amq.")) {
            throw Writes.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.PRECONDITION_FAILED,
                    path.get(1) + " is built in and stays.");
        }
        if (delete.ifEmpty() && writes.bindingsFrom(path.get(0), path.get(1), deadline) > 0) {
            throw Writes.refused(Status.FAILED_PRECONDITION, ErrorReasons.HAS_CHILDREN, id + " has bindings.");
        }
        return Map.of("if-unused", Boolean.toString(delete.ifEmpty()));
    }

    private Plan plan(ResourceId id, Operation operation, Deadline deadline) {
        KindId kind = id.kind();
        check(id, operation);
        Optional<JsonNode> current = writes.read(id, deadline);
        if (current.isEmpty() && !(operation instanceof Operation.Create)) {
            if (kind.equals(RabbitMqKinds.SHOVEL)
                    && writes.staticShovel(id.path().segments().get(0), id.name(), deadline)) {
                throw Writes.refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.PRECONDITION_FAILED,
                        id + " comes from the node's configuration; remove it there.");
            }
            throw RabbitMqErrors.fromStatus(404, "operation on " + id, Writes.objectPath(id), "", false);
        }
        Map<String, Object> before =
                current.map(json -> Writes.attributes(kind, json)).orElse(Map.of());
        long messages = current.map(json -> json.path(MESSAGE_COUNT).asLong(0)).orElse(0L);
        long ready = current.map(json -> json.path("messages_ready").asLong(0)).orElse(0L);
        List<AttributeChange> diff = new ArrayList<>();
        switch (operation) {
            case Operation.Create(Attributes attributes) ->
                attributes.asMap().forEach((k, v) -> diff.add(change(k, before.get(k), v)));
            case Operation.Update(Attributes patch) ->
                patch.asMap().forEach((k, v) -> {
                    if (!v.equals(before.get(k))) {
                        diff.add(change(k, before.get(k), v));
                    }
                });
            case Operation.Delete _ -> before.forEach((k, v) -> diff.add(change(k, v, null)));
            default -> {
                // A purge and a close change no attribute.
            }
        }
        if (kind.equals(RabbitMqKinds.VHOST) && operation instanceof Operation.Delete) {
            messages = writes.queues(id.name(), deadline).stream()
                    .mapToLong(q -> q.path(MESSAGE_COUNT).asLong(0))
                    .sum();
        }
        Count count =
                switch (operation) {
                    case Operation.Delete _ -> Count.estimate(messages);
                    case Operation.Purge _ -> Count.estimate(ready);
                    default -> Count.exact(0);
                };
        return new Plan(current, Writes.etag(id, current), diff, new Estimate(count, MESSAGES), ready);
    }

    private static AttributeChange change(String key, Object before, Object after) {
        return new AttributeChange(key, Optional.ofNullable(before), Optional.ofNullable(after));
    }

    /** Refuses, without a call, a verb the kind does not offer. */
    private static void check(ResourceId id, Operation operation) {
        KindId kind = id.kind();
        boolean allowed =
                switch (operation) {
                    case Operation.Create _ -> CREATE.contains(kind);
                    case Operation.Update _ -> UPDATE.contains(kind);
                    case Operation.Delete _ -> CREATE.contains(kind) || kind.equals(RabbitMqKinds.SHOVEL);
                    case Operation.Purge _ -> PURGE.contains(kind);
                    case Operation.Close _ -> kind.equals(RabbitMqKinds.CONNECTION);
                    default -> false;
                };
        if (!allowed) {
            throw unsupported(id, operation);
        }
        if (operation instanceof Operation.Update(Attributes patch) && patch.isEmpty()) {
            throw RabbitMqErrors.invalid("An update of " + id + " names no attribute.");
        }
    }

    private static BrokerException unsupported(ResourceId id, Operation operation) {
        String why =
                switch (operation) {
                    case Operation.Purge _
                    when id.kind().equals(RabbitMqKinds.STREAM) ->
                        "a stream keeps its messages until retention removes them";
                    case Operation.Pause _ -> "RabbitMQ has no pause";
                    case Operation.Resume _ -> "RabbitMQ has no pause to resume from";
                    case Operation.Seek _ -> "a stream's offsets are kept by the client over AMQP 0-9-1";
                    case Operation.Truncate _ -> "streams are truncated by retention only";
                    case Operation.Reassign _ -> "the management API has no per-queue call to move members";
                    case Operation.ElectLeader _ -> "the management API has no per-queue call to elect a leader";
                    case Operation.Resolve _ -> "RabbitMQ has no in-doubt transactions to resolve";
                    default -> "the kind does not offer it";
                };
        return Writes.refused(
                Status.UNIMPLEMENTED,
                ErrorReasons.NOT_SUPPORTED,
                operation.getClass().getSimpleName() + " is not supported on " + id.kind() + ": " + why + ".");
    }
}
