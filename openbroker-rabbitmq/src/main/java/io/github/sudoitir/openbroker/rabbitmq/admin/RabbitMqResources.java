package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.Ordering;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

/**
 * {@link Resources}: dispatches a query by kind or class, validates its orderings and field selector, runs one
 * listing per kind and merges the pages.
 *
 * <p>Exchanges, queues, connections and channels are paged by the management API itself ({@code page},
 * {@code page_size}, a name regex, a sort); the other kinds are listed whole and cut here. A query for one kind keeps
 * the management API's paging; a query for a class merges the whole listings of its kinds and cuts them by offset.
 * A query with no limit reads everything.
 */
final class RabbitMqResources implements Resources {

    private static final String LISTING = "list resources";
    private static final String NAME = "name";
    private static final String VHOST = "vhost";
    /**
     * The attributes the management API leaves out of a listing made with {@code disable_stats}: a projection that
     * names any of them is listed with full statistics.
     */
    private static final Set<String> HEAVY_METRICS = Set.of(
            RabbitMqKinds.COMMITTED_OFFSET,
            RabbitMqKinds.FIRST_TIMESTAMP,
            "openbroker.backlog.size",
            "openbroker.backlog.age",
            "openbroker.enqueued",
            "openbroker.acknowledged",
            "openbroker.consumer.count");

    /**
     * Where a kind is listed: the cluster-wide path, the path within one virtual host, whether the management API
     * pages it, and the field selectors it takes with the JSON field each compares with.
     */
    private record Feed(String all, Function<String, String> inVhost, boolean paged, Map<String, String> fields) {}

    /** What one listing found, and the page number to resume at when the management API has more. */
    private record Found(List<Resource> items, int nextPage) {}

    private static final ResourcePage NONE = new ResourcePage(List.of(), Optional.empty());

    private final RabbitMqCluster cluster;
    private final Map<KindId, Feed> feeds = feeds();
    private final List<Kind> kinds = RabbitMqKinds.all();
    private final Vhosts vhosts;
    private final Exchanges exchanges;
    private final Queues queues;
    private final Bindings bindings;
    private final Policies policies;
    private final Permissions permissions;
    private final Users users;
    private final Clients clients;
    private final Shovels shovels;
    private final FederationLinks federationLinks;

    RabbitMqResources(RabbitMqCluster cluster) {
        this.cluster = cluster;
        this.vhosts = new Vhosts(cluster);
        this.exchanges = new Exchanges(cluster);
        this.queues = new Queues(cluster);
        this.bindings = new Bindings(cluster);
        this.policies = new Policies(cluster);
        this.permissions = new Permissions(cluster);
        this.users = new Users(cluster);
        this.clients = new Clients(cluster);
        this.shovels = new Shovels(cluster);
        this.federationLinks = new FederationLinks(cluster);
    }

    @Override
    public List<Kind> kinds() {
        return kinds;
    }

    @Override
    public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
        List<ResourcePage> pages = new ArrayList<>();
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (ResourceQuery query : queries) {
            try {
                pages.add(readOne(query));
                outcomes.putIfAbsent(RabbitMqCluster.CLUSTER, NodeOutcome.APPLIED);
            } catch (BrokerException e) {
                pages.add(NONE);
                outcomes.put(RabbitMqCluster.CLUSTER, NodeOutcome.FAILED);
                failures.put(RabbitMqCluster.CLUSTER, RabbitMqErrors.at(e, RabbitMqCluster.CLUSTER));
            }
        }
        return new Partial<>(pages, outcomes, failures);
    }

    private ResourcePage readOne(ResourceQuery query) {
        List<Kind> selected = select(query);
        Set<String> declared = new HashSet<>();
        selected.forEach(k -> declared.addAll(k.orderings()));
        for (Ordering ordering : query.orderBy()) {
            if (!declared.contains(ordering.field())) {
                throw RabbitMqErrors.invalid("No kind in this query declares an ordering by " + ordering.field() + ".");
            }
        }
        for (Kind kind : selected) {
            for (String key : query.fieldSelector().keySet()) {
                if (!feeds.get(kind.id()).fields().containsKey(key)) {
                    throw RabbitMqErrors.invalid(
                            kind.label().plural() + " cannot be selected by " + key + "; they can be by "
                                    + feeds.get(kind.id()).fields().keySet() + ".");
                }
            }
        }
        Set<String> nodes = nodes(query.scope());
        Listing.Cursor cursor = Listing.decode(query.continueToken());
        Deadline deadline = Deadlines.current(cluster.options());
        return cluster.call(LISTING, Optional.empty(), deadline, () -> {
            if (selected.size() == 1 && feeds.get(selected.getFirst().id()).paged()) {
                return pageOfOne(selected.getFirst().id(), query, nodes, cursor, deadline);
            }
            return pageOfMany(selected, query, nodes, cursor, deadline);
        });
    }

    private List<Kind> select(ResourceQuery query) {
        if (query.kind().isPresent()) {
            KindId id = query.kind().get();
            return List.of(kinds.stream()
                    .filter(k -> k.id().equals(id))
                    .findFirst()
                    .orElseThrow(() -> RabbitMqErrors.invalid(id + " is not a kind of RabbitMQ.")));
        }
        List<Kind> inClass = kinds.stream()
                .filter(k -> k.resourceClass() == query.resourceClass().orElseThrow())
                .toList();
        if (inClass.isEmpty()) {
            throw RabbitMqErrors.invalid("RabbitMQ has no resources of class "
                    + query.resourceClass().orElseThrow() + ".");
        }
        return inClass;
    }

    private static Set<String> nodes(Scope scope) {
        Set<String> nodes = new HashSet<>();
        if (scope instanceof Scope.Shards shards) {
            for (Shard shard : shards.shards()) {
                if (shard instanceof Shard.NodeInstance node) {
                    nodes.add(node.node().value());
                } else {
                    throw RabbitMqErrors.invalid("RabbitMQ has no partitions; scope work to nodes.");
                }
            }
        }
        return nodes;
    }

    // One kind that the management API pages: ask it for the page.

    private ResourcePage pageOfOne(
            KindId kind, ResourceQuery query, Set<String> nodes, Listing.Cursor cursor, Deadline deadline) {
        if (cursor.offset() != 0 || (query.continueToken().isPresent() && !cursor.server())) {
            throw RabbitMqErrors.invalid("The continue token is not one this listing issued.");
        }
        Found found = serverRead(kind, query, nodes, cursor.page(), query.limit(), deadline);
        Optional<String> next = found.nextPage() > 0
                ? Optional.of(Listing.token(new Listing.Cursor(true, found.nextPage())))
                : Optional.empty();
        return new ResourcePage(
                found.items().stream().map(r -> project(r, query)).toList(), next);
    }

    /** Reads pages from {@code first} until {@code limit} resources are found, or all of them when it is 0. */
    private Found serverRead(
            KindId kind, ResourceQuery query, Set<String> nodes, int first, int limit, Deadline deadline) {
        Feed feed = feeds.get(kind);
        Map<String, String> params = new LinkedHashMap<>();
        int size = limit > 0 ? Math.min(limit, Listing.MAX_PAGE_SIZE) : Listing.MAX_PAGE_SIZE;
        params.put("page_size", Integer.toString(size));
        params.put("sort", NAME);
        if (!query.orderBy().isEmpty() && query.orderBy().getFirst().descending()) {
            params.put("sort_reverse", "true");
        }
        query.namePattern().ifPresent(glob -> {
            params.put(NAME, regex(glob));
            params.put("use_regex", "true");
        });
        // A stream's counters and offsets are statistics too, and enable_queue_totals does not bring them back.
        if (isQueue(kind) && !kind.equals(RabbitMqKinds.STREAM) && reducedStats(query)) {
            params.put("disable_stats", "true");
            params.put("enable_queue_totals", "true");
        }
        String path = path(feed, query);
        List<Resource> out = new ArrayList<>();
        int page = first;
        while (true) {
            params.put("page", Integer.toString(page));
            JsonNode body =
                    cluster.management().get(path, params, deadline, LISTING).body();
            JsonNode items = body.isArray() ? body : body.path("items");
            int pages = body.isArray() ? 1 : body.path("page_count").asInt(1);
            for (JsonNode item : items) {
                if (selected(feed, kind, item, query)) {
                    Resource resource = read(kind, item);
                    if (onNodes(resource, nodes)) {
                        out.add(resource);
                    }
                }
            }
            if (page >= pages) {
                return new Found(out, 0);
            }
            page++;
            if (limit > 0 && !out.isEmpty()) {
                return new Found(out, page);
            }
        }
    }

    // Several kinds, or one that is listed whole: read all of it, order it, cut it by offset.

    private ResourcePage pageOfMany(
            List<Kind> selected, ResourceQuery query, Set<String> nodes, Listing.Cursor cursor, Deadline deadline) {
        if (cursor.server()) {
            throw RabbitMqErrors.invalid("The continue token is not one this listing issued.");
        }
        List<Resource> all = new ArrayList<>();
        for (Kind kind : selected) {
            if (feeds.get(kind.id()).paged()) {
                all.addAll(serverRead(kind.id(), query, nodes, 1, 0, deadline).items());
            } else {
                all.addAll(clientRead(kind.id(), query, nodes, deadline));
            }
        }
        Comparator<Resource> byName = Comparator.comparing(r -> r.id().name());
        boolean descending =
                !query.orderBy().isEmpty() && query.orderBy().getFirst().descending();
        Comparator<Resource> order =
                (descending ? byName.reversed() : byName).thenComparing(Comparator.comparing(r -> r.id().format()));
        all.sort(order);
        int from = Math.min(cursor.offset(), all.size());
        int to = query.limit() > 0 ? Math.min(all.size(), from + query.limit()) : all.size();
        Optional<String> next =
                to < all.size() ? Optional.of(Listing.token(new Listing.Cursor(false, to))) : Optional.empty();
        return new ResourcePage(
                all.subList(from, to).stream().map(r -> project(r, query)).toList(), next);
    }

    private List<Resource> clientRead(KindId kind, ResourceQuery query, Set<String> nodes, Deadline deadline) {
        Feed feed = feeds.get(kind);
        if (RabbitMqKinds.FEDERATION_LINK.equals(kind) && !cluster.features().federation()) {
            return List.of();
        }
        List<Resource> out = new ArrayList<>();
        Pattern name =
                query.namePattern().map(glob -> Pattern.compile(regex(glob))).orElse(null);
        List<Resource> found = RabbitMqKinds.SHOVEL.equals(kind)
                ? shovels(feed, query, deadline)
                : listed(kind, feed, query, deadline);
        for (Resource resource : found) {
            if ((name == null || name.matcher(resource.id().name()).matches()) && onNodes(resource, nodes)) {
                out.add(resource);
            }
        }
        return out;
    }

    private List<Resource> listed(KindId kind, Feed feed, ResourceQuery query, Deadline deadline) {
        List<Resource> out = new ArrayList<>();
        for (JsonNode item : array(feed, query, deadline)) {
            if (selected(feed, kind, item, query)) {
                out.add(read(kind, item));
            }
        }
        return out;
    }

    private List<Resource> shovels(Feed feed, ResourceQuery query, Deadline deadline) {
        Map<String, JsonNode> states = new LinkedHashMap<>();
        if (cluster.features().shovels()) {
            String statuses = query.fieldSelector().containsKey(VHOST)
                    ? "/api/shovels/"
                            + ManagementPaths.segment(query.fieldSelector().get(VHOST))
                    : "/api/shovels";
            for (JsonNode state : list(statuses, deadline)) {
                states.put(
                        state.path(VHOST).asString() + '\0' + state.path(NAME).asString(), state);
            }
        }
        List<Resource> out = new ArrayList<>();
        for (JsonNode parameter : array(feed, query, deadline)) {
            if (selected(feed, RabbitMqKinds.SHOVEL, parameter, query)) {
                JsonNode state = states.remove(parameter.path(VHOST).asString()
                        + '\0'
                        + parameter.path(NAME).asString());
                out.add(shovels.read(parameter, state == null ? cluster.mapper().missingNode() : state));
            }
        }
        for (JsonNode state : states.values()) {
            if ("static".equals(state.path("type").asString()) && selected(feed, RabbitMqKinds.SHOVEL, state, query)) {
                out.add(shovels.read(cluster.mapper().missingNode(), state));
            }
        }
        return out;
    }

    private JsonNode array(Feed feed, ResourceQuery query, Deadline deadline) {
        return list(path(feed, query), deadline);
    }

    private JsonNode list(String path, Deadline deadline) {
        JsonNode body =
                cluster.management().get(path, Map.of(), deadline, LISTING).body();
        if (!body.isArray()) {
            throw RabbitMqErrors.badResponse(path + " did not answer with a list.", null);
        }
        return body;
    }

    // Reading one JSON item into a resource of its kind.

    private Resource read(KindId kind, JsonNode item) {
        if (RabbitMqKinds.VHOST.equals(kind)) {
            return vhosts.read(item);
        } else if (RabbitMqKinds.EXCHANGE.equals(kind)) {
            return exchanges.read(item);
        } else if (isQueue(kind)) {
            return queues.read(item);
        } else if (RabbitMqKinds.BINDING.equals(kind) || RabbitMqKinds.EXCHANGE_BINDING.equals(kind)) {
            return bindings.read(item);
        } else if (RabbitMqKinds.POLICY.equals(kind) || RabbitMqKinds.OPERATOR_POLICY.equals(kind)) {
            return policies.read(kind, item);
        } else if (RabbitMqKinds.PERMISSION.equals(kind) || RabbitMqKinds.TOPIC_PERMISSION.equals(kind)) {
            return permissions.read(item);
        } else if (RabbitMqKinds.USER.equals(kind)) {
            return users.read(item);
        } else if (RabbitMqKinds.FEDERATION_LINK.equals(kind)) {
            return federationLinks.read(item);
        } else if (RabbitMqKinds.SHOVEL.equals(kind)) {
            return shovels.read(item);
        }
        return clients.read(item);
    }

    /** Whether {@code item} belongs to {@code kind} and matches the field selector. */
    private static boolean selected(Feed feed, KindId kind, JsonNode item, ResourceQuery query) {
        if (isQueue(kind)) {
            if (!Queues.kind(item.path("type").asString()).filter(kind::equals).isPresent()) {
                return false;
            }
        } else if (RabbitMqKinds.BINDING.equals(kind) || RabbitMqKinds.EXCHANGE_BINDING.equals(kind)) {
            if (Bindings.fromDefaultExchange(item) || !Bindings.kind(item).equals(kind)) {
                return false;
            }
        } else if (RabbitMqKinds.PERMISSION.equals(kind) && item.has("exchange")
                || RabbitMqKinds.TOPIC_PERMISSION.equals(kind) && !item.has("exchange")) {
            return false;
        }
        for (Map.Entry<String, String> selector : query.fieldSelector().entrySet()) {
            String field = feed.fields().get(selector.getKey());
            if (!selector.getValue().equals(ReadSupport.at(item, field).asString())) {
                return false;
            }
        }
        return true;
    }

    private static boolean onNodes(Resource resource, Set<String> nodes) {
        return nodes.isEmpty()
                || resource.placements().stream()
                        .anyMatch(p -> nodes.contains(p.node().value()));
    }

    private static String path(Feed feed, ResourceQuery query) {
        String vhost = query.fieldSelector().get(VHOST);
        return vhost == null || feed.inVhost() == null
                ? feed.all()
                : feed.inVhost().apply(ManagementPaths.segment(vhost));
    }

    private static Resource project(Resource resource, ResourceQuery query) {
        if (query.projection().isEmpty()) {
            return resource;
        }
        Attributes.Builder kept = Attributes.builder();
        resource.attributes().asMap().forEach((key, value) -> {
            if (query.projection().contains(key)) {
                kept.putRaw(key, value);
            }
        });
        return new Resource(
                resource.id(), kept.build(), resource.conditions(), resource.placements(), resource.observedAt());
    }

    private static boolean isQueue(KindId kind) {
        return RabbitMqKinds.CLASSIC_QUEUE.equals(kind)
                || RabbitMqKinds.QUORUM_QUEUE.equals(kind)
                || RabbitMqKinds.STREAM.equals(kind);
    }

    /** Whether the projection leaves out every statistic that the reduced queue listing does not carry. */
    private static boolean reducedStats(ResourceQuery query) {
        return !query.projection().isEmpty() && query.projection().stream().noneMatch(HEAVY_METRICS::contains);
    }

    /** The regular expression of a glob ({@code *} and {@code ?}), anchored, in the syntax RabbitMQ and Java share. */
    static String regex(String glob) {
        StringBuilder out = new StringBuilder("^");
        glob.codePoints().forEach(c -> {
            switch (c) {
                case '*' -> out.append(".*");
                case '?' -> out.append('.');
                default -> {
                    if (!Character.isLetterOrDigit(c)) {
                        out.append('\\');
                    }
                    out.appendCodePoint(c);
                }
            }
        });
        return out.append('$').toString();
    }

    private static Function<String, String> underVhost(String collection) {
        return vhost -> "/api/vhosts/" + vhost + "/" + collection;
    }

    private static Map<KindId, Feed> feeds() {
        Map<String, String> vhostOnly = Map.of(VHOST, VHOST);
        Map<KindId, Feed> out = new LinkedHashMap<>();
        out.put(RabbitMqKinds.VHOST, new Feed("/api/vhosts", null, false, Map.of()));
        out.put(RabbitMqKinds.EXCHANGE, new Feed("/api/exchanges", v -> "/api/exchanges/" + v, true, vhostOnly));
        Feed queue = new Feed("/api/queues", v -> "/api/queues/" + v, true, vhostOnly);
        out.put(RabbitMqKinds.CLASSIC_QUEUE, queue);
        out.put(RabbitMqKinds.QUORUM_QUEUE, queue);
        out.put(RabbitMqKinds.STREAM, queue);
        Feed binding = new Feed("/api/bindings", v -> "/api/bindings/" + v, false, vhostOnly);
        out.put(RabbitMqKinds.BINDING, binding);
        out.put(RabbitMqKinds.EXCHANGE_BINDING, binding);
        out.put(
                RabbitMqKinds.SHOVEL,
                new Feed("/api/parameters/shovel", v -> "/api/parameters/shovel/" + v, false, vhostOnly));
        out.put(
                RabbitMqKinds.FEDERATION_LINK,
                new Feed("/api/federation-links", v -> "/api/federation-links/" + v, false, vhostOnly));
        out.put(RabbitMqKinds.POLICY, new Feed("/api/policies", v -> "/api/policies/" + v, false, vhostOnly));
        out.put(
                RabbitMqKinds.OPERATOR_POLICY,
                new Feed("/api/operator-policies", v -> "/api/operator-policies/" + v, false, vhostOnly));
        Map<String, String> byUser = Map.of(VHOST, VHOST, "user", "user");
        out.put(RabbitMqKinds.PERMISSION, new Feed("/api/permissions", underVhost("permissions"), false, byUser));
        out.put(
                RabbitMqKinds.TOPIC_PERMISSION,
                new Feed("/api/topic-permissions", underVhost("topic-permissions"), false, byUser));
        out.put(RabbitMqKinds.USER, new Feed("/api/users", null, false, Map.of()));
        out.put(RabbitMqKinds.CONNECTION, new Feed("/api/connections", underVhost("connections"), true, vhostOnly));
        out.put(RabbitMqKinds.CHANNEL, new Feed("/api/channels", underVhost("channels"), true, vhostOnly));
        out.put(
                RabbitMqKinds.CONSUMER,
                new Feed(
                        "/api/consumers",
                        v -> "/api/consumers/" + v,
                        false,
                        Map.of(VHOST, "queue.vhost", "queue", "queue.name")));
        return out;
    }
}
