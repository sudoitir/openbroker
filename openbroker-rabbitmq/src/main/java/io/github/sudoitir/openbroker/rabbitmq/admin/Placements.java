package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.PlacementRole;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/** Queue placements (leader, followers, instance) and the {@code UNDER_REPLICATED} condition. */
final class Placements {

    private Placements() {}

    /**
     * The placements of the queue of kind {@code kind} that {@code queue} describes. A quorum queue or stream has its
     * leader and followers, each in sync when it is online; a classic queue is one instance, or on 3.13 a leader and
     * its mirrors when it has any.
     */
    static List<Placement> of(KindId kind, JsonNode queue) {
        if (replicated(kind)) {
            List<String> members = ReadSupport.names(queue.path("members"));
            if (!members.isEmpty()) {
                Optional<PartitionId> partition =
                        RabbitMqKinds.STREAM.equals(kind) ? Optional.of(PartitionId.of("0")) : Optional.empty();
                String leader = queue.path("leader").asString();
                boolean known = queue.has("online");
                Set<String> online = new HashSet<>(ReadSupport.names(queue.path("online")));
                List<Placement> out = new ArrayList<>();
                for (String member : members) {
                    PlacementRole role = member.equals(leader) ? PlacementRole.LEADER : PlacementRole.FOLLOWER;
                    Optional<Boolean> inSync = known ? Optional.of(online.contains(member)) : Optional.empty();
                    Placement placement = new Placement(NodeId.of(member), partition, role, inSync, Attributes.empty());
                    if (role == PlacementRole.LEADER) {
                        out.addFirst(placement);
                    } else {
                        out.add(placement);
                    }
                }
                return out;
            }
        }
        Optional<String> node = ReadSupport.text(queue, "node");
        if (node.isEmpty()) {
            return List.of();
        }
        List<String> mirrors = ReadSupport.names(queue.path("slave_nodes"));
        if (replicated(kind) || mirrors.isEmpty()) {
            return List.of(Placement.instance(NodeId.of(node.get()), Attributes.empty()));
        }
        Set<String> synchronised = new HashSet<>(ReadSupport.names(queue.path("synchronised_slave_nodes")));
        List<Placement> out = new ArrayList<>();
        out.add(new Placement(
                NodeId.of(node.get()), Optional.empty(), PlacementRole.LEADER, Optional.of(true), Attributes.empty()));
        for (String mirror : mirrors) {
            out.add(new Placement(
                    NodeId.of(mirror),
                    Optional.empty(),
                    PlacementRole.FOLLOWER,
                    Optional.of(synchronised.contains(mirror)),
                    Attributes.empty()));
        }
        return out;
    }

    /**
     * {@code UNDER_REPLICATED}: true while fewer members are online (or mirrors synchronised) than the queue has.
     * Empty for a queue that is not replicated or whose listing does not say.
     */
    static Optional<Condition> underReplicated(KindId kind, JsonNode queue) {
        List<Placement> placements = of(kind, queue);
        boolean replicated = placements.stream().anyMatch(p -> p.role() != PlacementRole.INSTANCE);
        if (!replicated || placements.stream().anyMatch(p -> p.inSync().isEmpty())) {
            return Optional.empty();
        }
        List<String> behind = placements.stream()
                .filter(p -> !p.inSync().orElse(true))
                .map(p -> p.node().value())
                .toList();
        if (behind.isEmpty()) {
            return Optional.of(
                    new Condition(ConditionTypes.UNDER_REPLICATED, ConditionStatus.FALSE, "", "", Optional.empty()));
        }
        return Optional.of(new Condition(
                ConditionTypes.UNDER_REPLICATED,
                ConditionStatus.TRUE,
                "member_out_of_sync",
                (placements.size() - behind.size()) + " of " + placements.size() + " members are in sync; behind: "
                        + String.join(", ", behind) + ".",
                Optional.empty()));
    }

    private static boolean replicated(KindId kind) {
        return RabbitMqKinds.QUORUM_QUEUE.equals(kind) || RabbitMqKinds.STREAM.equals(kind);
    }
}
