package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.Ordering;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        Deadline deadline = Deadlines.current(cluster.options());
        List<ResourcePage> pages = new ArrayList<>();
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (ResourceQuery query : queries) {
            Partial<ResourcePage> one = Outcomes.of(
                    new ResourcePage(List.of(), Optional.empty()),
                    () -> handlers.call("resources.read", Optional.empty(), deadline, context -> read(context, query)));
            pages.add(one.value());
            one.outcomes().forEach((n, o) -> outcomes.merge(n, o, (a, b) -> a == NodeOutcome.APPLIED ? b : a));
            failures.putAll(one.failures());
        }
        return new Partial<>(pages, outcomes, failures);
    }

    private ResourcePage read(Context context, ResourceQuery query) {
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
        byHandler.forEach(
                (handler, cs) -> handler.resources(context, cs, listing).forEach(r -> read.put(r.id(), r)));
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
