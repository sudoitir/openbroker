package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Lists Artemis resources. A paged list ({@code limit} or a continue token) reads one node; without a
 * limit every page of every node in scope is read.
 */
public final class ArtemisResources implements Resources {

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
        List<ResourcePage> pages = new ArrayList<>();
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (ResourceQuery query : queries) {
            Partial<ResourcePage> one = read(query, deadline);
            pages.add(one.value());
            one.outcomes().forEach((n, o) -> outcomes.merge(n, o, (a, b) -> a == NodeOutcome.APPLIED ? b : a));
            failures.putAll(one.failures());
        }
        return new Partial<>(pages, outcomes, failures);
    }

    private Partial<ResourcePage> read(ResourceQuery query, Deadline deadline) {
        Queries.requireNoOrdering(query);
        KindId kind = query.kind().orElseGet(() -> kindOf(query.resourceClass().orElseThrow()));
        Catalog.kind(kind);
        List<NodeHandle> nodes = cluster.nodes(query.scope());
        boolean paged = query.limit() > 0 || query.continueToken().isPresent();
        if (paged && nodes.size() != 1) {
            throw Queries.invalid("A paged Artemis list reads one node; scope the query to one node.");
        }
        Predicate<String> names = Queries.names(query);
        if (kind.equals(ArtemisKinds.QUEUE)) {
            return queues(nodes, query, paged, names, deadline);
        }
        throw Unimplemented.of("list " + kind + " yet");
    }

    private Partial<ResourcePage> queues(
            List<NodeHandle> nodes, ResourceQuery query, boolean paged, Predicate<String> names, Deadline deadline) {
        int page = paged ? Queries.page(query) : -1;
        int size = paged ? (query.limit() > 0 ? query.limit() : 200) : -1;
        Optional<String>[] next = new Optional[] {Optional.empty()};
        Partial<List<Resource>> rows = Fanout.each(nodes, cluster.options(), node -> {
            JolokiaClient client = cluster.jolokia(node);
            ListOps.Page p = ListOps.fetch(client, deadline, "listQueues", ListOps.ALL, page, size);
            Instant now = cluster.options().clock().instant();
            List<Resource> parsed = QueueRows.parse(p.data(), node.id(), now);
            if (paged && !parsed.isEmpty() && (long) page * size < p.count()) {
                next[0] = Optional.of(String.valueOf(page + 1));
            }
            return parsed.stream().filter(r -> names.test(r.id().name())).toList();
        });
        return rows.map(items -> new ResourcePage(items, next[0]));
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
