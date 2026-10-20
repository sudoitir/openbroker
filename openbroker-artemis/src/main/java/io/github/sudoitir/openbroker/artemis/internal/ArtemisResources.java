package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import tools.jackson.databind.JsonNode;

/**
 * Lists Artemis resources. A paged list ({@code limit} or a continue token) reads one node; without a
 * limit every page of every node in scope is read.
 */
public final class ArtemisResources implements Resources {

    /** The page size when a paged query sets no limit. */
    private static final int DEFAULT_PAGE = 200;

    private final Cluster cluster;

    public ArtemisResources(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public List<Kind> kinds() {
        return Catalog.KINDS;
    }

    @Override
    public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
        Deadline deadline = Deadlines.current(cluster.options());
        List<ResourcePage> pages = new ArrayList<>(queries.size());
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        List<Integer> batched = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            pages.add(null);
            if (batchable(queries.get(i))) {
                batched.add(i);
            }
        }
        if (batched.size() > 1) {
            Partial<List<ResourcePage>> together =
                    listedTogether(batched.stream().map(queries::get).toList(), deadline);
            for (int j = 0; j < batched.size(); j++) {
                pages.set(batched.get(j), together.value().get(j));
            }
            merge(together, outcomes, failures);
        } else {
            batched = List.of();
        }
        for (int i = 0; i < queries.size(); i++) {
            if (!batched.contains(i)) {
                Partial<ResourcePage> one = read(queries.get(i), deadline);
                pages.set(i, one.value());
                merge(one, outcomes, failures);
            }
        }
        return new Partial<>(pages, outcomes, failures);
    }

    private static void merge(
            Partial<?> one, Map<NodeId, NodeOutcome> outcomes, Map<NodeId, BrokerException> failures) {
        one.outcomes().forEach((n, o) -> outcomes.merge(n, o, (a, b) -> a == NodeOutcome.APPLIED ? b : a));
        failures.putAll(one.failures());
    }

    /** A whole list of a kind Artemis lists with a {@code listX} operation: it can share a request. */
    private static boolean batchable(ResourceQuery query) {
        boolean paged = query.limit() > 0 || query.continueToken().isPresent();
        return !paged
                && query.orderBy().isEmpty()
                && ListRows.listing(kindOf(query)).isPresent();
    }

    /**
     * Several whole lists in one request per node: the queries a node is in scope of go to it in one
     * batch, so reading a connection, its sessions and their consumers costs one call per node.
     */
    private Partial<List<ResourcePage>> listedTogether(List<ResourceQuery> queries, Deadline deadline) {
        Map<NodeId, NodeHandle> union = new LinkedHashMap<>();
        Map<Integer, Set<NodeId>> scopes = new LinkedHashMap<>();
        for (int i = 0; i < queries.size(); i++) {
            Set<NodeId> ids = new LinkedHashSet<>();
            for (NodeHandle n : cluster.nodes(queries.get(i).scope())) {
                union.putIfAbsent(n.id(), n);
                ids.add(n.id());
            }
            scopes.put(i, ids);
        }
        Instant now = cluster.options().clock().instant();
        Partial<List<Map.Entry<Integer, List<Resource>>>> rows =
                Fanout.each(List.copyOf(union.values()), cluster.options(), node -> {
                    NodeJolokia jolokia = cluster.jolokia(node, deadline);
                    String broker = jolokia.brokerName();
                    List<Integer> mine = new ArrayList<>();
                    List<JolokiaRequest> requests = new ArrayList<>();
                    for (int i = 0; i < queries.size(); i++) {
                        if (scopes.get(i).contains(node.id())) {
                            ResourceQuery q = queries.get(i);
                            KindId kind = kindOf(q);
                            mine.add(i);
                            requests.add(JolokiaRequest.exec(
                                    broker,
                                    ListRows.listing(kind).orElseThrow().operation() + "(java.lang.String,int,int)",
                                    Filters.options(kind, q),
                                    -1,
                                    -1));
                        }
                    }
                    List<JolokiaResponse> responses = jolokia.batch(requests);
                    List<Map.Entry<Integer, List<Resource>>> out = new ArrayList<>();
                    for (int j = 0; j < mine.size(); j++) {
                        ResourceQuery q = queries.get(mine.get(j));
                        JolokiaResponse response = j < responses.size() ? responses.get(j) : null;
                        if (response == null || !response.ok()) {
                            throw response == null
                                    ? Errors.badResponse(
                                            node.id(), "The broker returned fewer results than lists.", null)
                                    : Errors.refusal(node.id(), response, "list");
                        }
                        JsonNode envelope = jolokia.parsed(response);
                        List<Resource> parsed = ListRows.parse(
                                kindOf(q), envelope == null ? null : envelope.get("data"), node.id(), now);
                        out.add(Map.entry(mine.get(j), Filters.keep(q, parsed)));
                    }
                    return out;
                });
        List<List<Resource>> perQuery = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            perQuery.add(new ArrayList<>());
        }
        rows.value().forEach(e -> perQuery.get(e.getKey()).addAll(e.getValue()));
        return rows.map(ignored -> perQuery.stream()
                .map(items -> new ResourcePage(items, Optional.empty()))
                .toList());
    }

    private Partial<ResourcePage> read(ResourceQuery query, Deadline deadline) {
        Queries.requireNoOrdering(query);
        KindId kind = kindOf(query);
        Catalog.kind(kind);
        List<NodeHandle> nodes = cluster.nodes(query.scope());
        boolean paged = query.limit() > 0 || query.continueToken().isPresent();
        if (paged && nodes.size() != 1) {
            throw Queries.invalid("A paged Artemis list reads one node; scope the query to one node.");
        }
        Predicate<String> names = Queries.names(query);
        Optional<ListRows.Listing> listing = ListRows.listing(kind);
        if (listing.isPresent()) {
            return listed(kind, listing.get().operation(), nodes, query, paged, names, deadline);
        }
        if (paged) {
            throw Queries.invalid("Artemis lists " + kind + " in one page; leave out the limit.");
        }
        if (kind.equals(ArtemisKinds.DIVERT) || kind.equals(ArtemisKinds.BRIDGE)) {
            return routes(kind, nodes, names, deadline);
        }
        throw Unimplemented.of("list " + kind + "; read it through ArtemisManagement");
    }

    private Partial<ResourcePage> listed(
            KindId kind,
            String operation,
            List<NodeHandle> nodes,
            ResourceQuery query,
            boolean paged,
            Predicate<String> names,
            Deadline deadline) {
        int page = paged ? Queries.page(query) : -1;
        int size = paged ? (query.limit() > 0 ? query.limit() : DEFAULT_PAGE) : -1;
        AtomicReference<Optional<String>> next = new AtomicReference<>(Optional.empty());
        Partial<List<Resource>> rows = Fanout.each(nodes, cluster.options(), node -> {
            ArtemisLists.Page p = ArtemisLists.fetch(
                    cluster.jolokia(node, deadline), operation, Filters.options(kind, query), page, size);
            List<Resource> parsed = ListRows.parse(
                    kind, p.data(), node.id(), cluster.options().clock().instant());
            if (paged && !parsed.isEmpty() && (long) page * size < p.count()) {
                next.set(Optional.of(String.valueOf(page + 1)));
            }
            return Filters.keep(query, parsed);
        });
        return rows.map(items -> new ResourcePage(items, next.get()));
    }

    private Partial<ResourcePage> routes(
            KindId kind, List<NodeHandle> nodes, Predicate<String> names, Deadline deadline) {
        boolean diverts = kind.equals(ArtemisKinds.DIVERT);
        Partial<List<Resource>> rows = Fanout.each(nodes, cluster.options(), node -> {
            NodeJolokia jolokia = cluster.jolokia(node, deadline);
            String broker = jolokia.brokerName();
            List<String> mbeans = jolokia.search(
                    diverts ? ArtemisMBeans.divertsPattern(broker) : ArtemisMBeans.bridgesPattern(broker));
            Instant now = cluster.options().clock().instant();
            return ArtemisLists.readAll(
                            jolokia,
                            mbeans,
                            m -> Optional.ofNullable(
                                    diverts
                                            ? RouteRows.divert(m, node.id(), now)
                                            : RouteRows.bridge(m, node.id(), now)))
                    .stream()
                    .flatMap(Optional::stream)
                    .filter(r -> names.test(r.id().name()))
                    .toList();
        });
        return rows.map(items -> new ResourcePage(items, Optional.empty()));
    }

    private static KindId kindOf(ResourceQuery query) {
        return query.kind().orElseGet(() -> kindOf(query.resourceClass().orElseThrow()));
    }

    private static KindId kindOf(ResourceClass resourceClass) {
        return switch (resourceClass) {
            case SUBSCRIPTION -> ArtemisKinds.QUEUE;
            case DESTINATION -> ArtemisKinds.ADDRESS;
            case CONNECTION -> ArtemisKinds.CONNECTION;
            case CHANNEL -> ArtemisKinds.SESSION;
            case CONSUMER -> ArtemisKinds.CONSUMER;
            case PRODUCER -> ArtemisKinds.PRODUCER;
            case POLICY -> ArtemisKinds.ADDRESS_SETTING;
            case ACCESS_RULE -> ArtemisKinds.SECURITY_SETTING;
            case PRINCIPAL -> ArtemisKinds.USER;
            case TRANSACTION -> ArtemisKinds.TRANSACTION;
            case ROUTE -> throw Queries.invalid("Artemis has two route kinds; name artemis.divert or artemis.bridge.");
            case NAMESPACE, PARTITION, CONSUMER_GROUP -> throw Unimplemented.of("hold a " + resourceClass);
        };
    }
}
