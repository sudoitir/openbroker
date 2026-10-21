package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Serving;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Where a lost link goes: its own node, then the other members of the node's high-availability
 * group that serve now. A group is what {@code Membership.group} says: the broker's node id, which a
 * primary and the backup that shares its journal both report. A backup that is not serving yet has
 * no node id to report, so a configured backup is taken for a member of any group it cannot be told
 * apart from. The members are the nodes of the connection's {@code ClusterSpec} with a Core endpoint.
 */
final class HaGroups {

    private final Supplier<List<NodeHandle>> members;
    private final Supplier<List<NodeDescription>> describe;
    private final Map<NodeId, String> groups = new ConcurrentHashMap<>();

    /**
     * @param members the nodes of the connection
     * @param describe describes the nodes that answer, as {@code Nodes.describe} does
     */
    HaGroups(Supplier<List<NodeHandle>> members, Supplier<List<NodeDescription>> describe) {
        this.members = members;
        this.describe = describe;
    }

    /**
     * Notes the groups of the connection's nodes, so that a node which is down later is still known
     * to belong to its group. Done until the node's own group is known; best effort, and a node that
     * does not answer is skipped.
     */
    void learn(NodeHandle node) {
        if (members.get().size() > 1 && !groups.containsKey(node.id())) {
            described();
        }
    }

    /**
     * The node a new link scoped to {@code node} opens on: the member of its group that serves now,
     * which is not {@code node} while a backup has taken over from it, or {@code node} itself.
     */
    NodeHandle serving(NodeHandle node) {
        return candidates(node, node).stream().findFirst().orElse(node);
    }

    /**
     * The nodes to try, best first: the members that report serving, then the node the link last
     * served from, its own node and the rest.
     */
    List<NodeHandle> candidates(NodeHandle origin, NodeHandle lastServed) {
        List<NodeHandle> all =
                members.get().stream().filter(n -> n.core().isPresent()).toList();
        if (all.size() < 2) {
            return List.of(origin);
        }
        Map<NodeId, NodeDescription> described = described();
        String group = groups.get(origin.id());
        Set<NodeHandle> serving = new LinkedHashSet<>();
        Set<NodeHandle> others = new LinkedHashSet<>();
        others.add(lastServed);
        others.add(origin);
        for (NodeHandle n : all) {
            NodeDescription d = described.get(n.id());
            boolean member = n.id().equals(origin.id())
                    || (group != null && (group.equals(groups.get(n.id())) || unnamedBackup(d)));
            if (!member) {
                continue;
            }
            if (d != null && d.membership().serving() == Serving.ACTIVE) {
                serving.add(n);
            } else {
                others.add(n);
            }
        }
        List<NodeHandle> ordered = new ArrayList<>(serving);
        others.stream().filter(n -> !ordered.contains(n)).forEach(ordered::add);
        return ordered.stream().filter(n -> n.core().isPresent()).toList();
    }

    private static boolean unnamedBackup(NodeDescription description) {
        return description != null
                && description.membership().group().isEmpty()
                && description.membership().designation() == Designation.BACKUP;
    }

    private Map<NodeId, NodeDescription> described() {
        Map<NodeId, NodeDescription> out = new LinkedHashMap<>();
        try {
            for (NodeDescription d : describe.get()) {
                out.put(d.node(), d);
                Optional<String> group = d.membership().group();
                group.ifPresent(g -> groups.put(d.node(), g));
            }
        } catch (RuntimeException e) {
            // Management is down too; the node's own endpoint is the only candidate known to be right.
        }
        return out;
    }
}
