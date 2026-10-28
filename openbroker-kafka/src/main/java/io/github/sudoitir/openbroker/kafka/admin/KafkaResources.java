package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Ordering;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.TopicPartition;

/**
 * The Kafka kinds and their resources. A listing is in three steps: the handlers say which resources
 * match, the page is cut from them, and only the page has its details read, so a long listing
 * costs a page of reads and not one per resource.
 */
public final class KafkaResources implements Resources {

    private final KafkaCluster cluster;
    private final Handlers handlers;

    KafkaResources(KafkaCluster cluster, Handlers handlers) {
        this.cluster = cluster;
        this.handlers = handlers;
    }

    @Override
    public List<Kind> kinds() {
        Deadline deadline = Deadlines.current(cluster.options());
        return handlers.call("resources.kinds", Optional.empty(), deadline, handlers::kinds);
    }

    @Override
    public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
        return read(queries, true);
    }

    /**
     * The same listings with only what a listing gives for free: each resource's id and where it
     * lives, none of its details. It is what the watch reads every interval.
     */
    Resources ids() {
        return new Resources() {
            @Override
            public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
                return KafkaResources.this.read(queries, false);
            }

            @Override
            public List<Kind> kinds() {
                return KafkaResources.this.kinds();
            }
        };
    }

    /**
     * Reads the queries in one call to the cluster, so that they share their rounds: the groups, topics
     * and offsets the queries name are asked for together, once. A query that fails fails alone.
     */
    private Partial<List<ResourcePage>> read(List<ResourceQuery> queries, boolean details) {
        Deadline deadline = Deadlines.current(cluster.options());
        ResourcePage none = new ResourcePage(List.of(), Optional.empty());
        try {
            return handlers.callReading("resources.read", deadline, context -> {
                prefetch(context, queries, details);
                return readEach(context, queries, none, details);
            });
        } catch (BrokerException e) {
            // The call itself was refused or ran out of time: every query has the same failure.
            return Outcomes.of(queries.stream().map(q -> none).toList(), () -> {
                throw e;
            });
        }
    }

    private Partial<List<ResourcePage>> readEach(
            Context context, List<ResourceQuery> queries, ResourcePage none, boolean details) {
        List<ResourcePage> pages = new ArrayList<>();
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (ResourceQuery query : queries) {
            Partial<ResourcePage> one = Outcomes.of(none, () -> read(context, query, details));
            pages.add(one.value());
            one.outcomes().forEach((n, o) -> outcomes.merge(n, o, (a, b) -> a == NodeOutcome.APPLIED ? b : a));
            failures.putAll(one.failures());
        }
        return new Partial<>(pages, outcomes, failures);
    }

    /**
     * Asks, for all the queries at once, what each would ask alone: the descriptions and offsets of the
     * groups they name and the partitions and log offsets of their topics. What cannot be read here is
     * left to the query that needs it, which reports it.
     */
    private void prefetch(Context context, List<ResourceQuery> queries, boolean details) {
        Set<String> groups = new LinkedHashSet<>();
        Set<String> topics = new LinkedHashSet<>();
        Map<KindId, Set<String>> groupsByKind = new LinkedHashMap<>();
        for (ResourceQuery query : queries) {
            Optional<KindId> kind = query.kind();
            if (kind.isEmpty()) {
                continue;
            }
            String group = query.fieldSelector().get("group");
            if (group != null && isSubscription(kind.get())) {
                groups.add(group);
                groupsByKind
                        .computeIfAbsent(kind.get(), k -> new LinkedHashSet<>())
                        .add(group);
                String topic = query.fieldSelector().get("topic");
                if (topic != null) {
                    topics.add(topic);
                }
            }
        }
        try {
            if (!groups.isEmpty()) {
                List<GroupListing> listed = GroupInfo.list(context).stream()
                        .filter(l -> groups.contains(l.groupId()))
                        .toList();
                GroupInfo.describe(context, listed);
                for (Map.Entry<KindId, Set<String>> e : groupsByKind.entrySet()) {
                    ((Subscriptions) handlers.of(context, e.getKey())).positions(context, e.getValue());
                }
            }
            if (details && !topics.isEmpty()) {
                List<TopicPartition> all = Offsets.partitions(context, topics).values().stream()
                        .flatMap(List::stream)
                        .toList();
                Offsets.earliest(context, all);
                Offsets.latest(context, all);
            }
        } catch (BrokerException _) {
            // The query that needs it asks again and reports the failure itself.
        }
    }

    private static boolean isSubscription(KindId kind) {
        return kind.equals(KafkaKinds.GROUP_TOPIC) || kind.equals(KafkaKinds.SHARE_GROUP_TOPIC);
    }

    private ResourcePage read(Context context, ResourceQuery query, boolean details) {
        List<Handler> selected = query.kind().isPresent()
                ? List.of(handlers.of(context, query.kind().get()))
                : handlers.ofClass(context, query.resourceClass().orElseThrow());
        Set<String> declared = new HashSet<>();
        selected.forEach(h -> declared.addAll(h.kind().orderings()));
        for (Ordering ordering : query.orderBy()) {
            if (!declared.contains(ordering.field())) {
                throw Attrs.invalid("No kind in this query declares an ordering by " + ordering.field() + ".");
            }
        }
        Listing listing = new Listing(query);
        List<Map.Entry<Handler, Candidate>> candidates = new ArrayList<>();
        for (Handler handler : selected) {
            for (String key : query.fieldSelector().keySet()) {
                if (!handler.selectors().contains(key)) {
                    throw Attrs.invalid(handler.kind().label().plural() + " cannot be selected by " + key
                            + "; they can be by " + handler.selectors() + ".");
                }
            }
            handler.candidates(context, listing).forEach(c -> candidates.add(Map.entry(handler, c)));
        }
        Comparator<Map.Entry<Handler, Candidate>> order =
                Comparator.comparing(e -> e.getValue().id().format());
        if (query.orderBy().stream().anyMatch(o -> o.field().equals("name"))) {
            Comparator<Map.Entry<Handler, Candidate>> byName =
                    Comparator.comparing(e -> e.getValue().id().name());
            order = query.orderBy().getFirst().descending()
                    ? byName.reversed().thenComparing(order)
                    : byName.thenComparing(order);
        }
        candidates.sort(order);
        int from = start(query);
        int to = query.limit() > 0 ? Math.min(candidates.size(), from + query.limit()) : candidates.size();
        List<Map.Entry<Handler, Candidate>> page = from >= candidates.size() ? List.of() : candidates.subList(from, to);
        Map<Handler, List<Candidate>> byHandler = new LinkedHashMap<>();
        page.forEach(e ->
                byHandler.computeIfAbsent(e.getKey(), h -> new ArrayList<>()).add(e.getValue()));
        Map<ResourceId, Resource> read = new LinkedHashMap<>();
        byHandler.forEach((handler, cs) -> {
            if (details) {
                handler.resources(context, cs, listing).forEach(r -> read.put(r.id(), r));
            } else {
                // The watch reports that a resource exists, not where its replicas are: every one is on the cluster.
                cs.forEach(c -> read.put(
                        c.id(),
                        new Resource(
                                c.id(),
                                c.attributes(),
                                List.of(),
                                List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                                context.now())));
            }
        });
        List<Resource> items = new ArrayList<>();
        page.forEach(e -> {
            Resource r = read.get(e.getValue().id());
            if (r != null) {
                items.add(r);
            }
        });
        Optional<String> next = to < candidates.size() ? Optional.of(String.valueOf(to)) : Optional.empty();
        return new ResourcePage(items, next);
    }

    private static int start(ResourceQuery query) {
        return query.continueToken()
                .map(token -> {
                    try {
                        int offset = Integer.parseInt(token);
                        if (offset < 0) {
                            throw new NumberFormatException(token);
                        }
                        return offset;
                    } catch (NumberFormatException e) {
                        throw KafkaErrors.refused(
                                Status.INVALID_ARGUMENT,
                                ErrorReasons.INVALID_ARGUMENT,
                                "Not a continue token: " + token);
                    }
                })
                .orElse(0);
    }
}
