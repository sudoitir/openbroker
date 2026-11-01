package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import tools.jackson.databind.JsonNode;

/**
 * A per-virtual-host map from queue name to queue type from a reduced listing, kept for the watch interval, so
 * bindings, consumers and events can name a queue's kind. A queue the listing does not hold (just deleted, or of a
 * type this driver does not list) is taken for a classic queue.
 */
final class QueueTypes {

    private record Listed(Map<String, KindId> kinds, Instant at) {}

    private final RabbitMqCluster cluster;
    private final Map<String, Listed> byVhost = new ConcurrentHashMap<>();

    QueueTypes(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    KindId kindOf(String vhost, String queue) {
        Listed listed = byVhost.get(vhost);
        if (listed == null || stale(listed)) {
            listed = load(vhost);
        } else if (!listed.kinds().containsKey(queue)) {
            // A queue created since the listing: look once more, not on every miss within the same instant.
            Instant now = cluster.options().clock().instant();
            if (Duration.between(listed.at(), now).compareTo(Duration.ofMillis(500)) > 0) {
                listed = load(vhost);
            }
        }
        KindId listedKind = listed.kinds().get(queue);
        if (listedKind != null) {
            return listedKind;
        }
        // The listing lags a queue that was just declared: ask for this one queue, which the broker answers at once.
        return one(vhost, queue).orElse(RabbitMqKinds.CLASSIC_QUEUE);
    }

    /**
     * Whether the kind of a queue is known for certain: from the listing, or else from the queue itself. Empty when the
     * queue does not exist (any more), so a caller does not keep a guess.
     */
    Optional<KindId> knownKindOf(String vhost, String queue) {
        Listed listed = byVhost.get(vhost);
        if (listed == null || stale(listed)) {
            listed = load(vhost);
        }
        KindId listedKind = listed.kinds().get(queue);
        return listedKind != null ? Optional.of(listedKind) : one(vhost, queue);
    }

    private Optional<KindId> one(String vhost, String queue) {
        try {
            JsonNode body = cluster.management()
                    .get(
                            "/api/queues/" + ManagementPaths.segment(vhost) + "/" + ManagementPaths.segment(queue),
                            Map.of("columns", "name,type", "disable_stats", "true"),
                            Deadlines.current(cluster.options()),
                            "read a queue's type")
                    .body();
            return kind(body.path("type").asString());
        } catch (BrokerException e) {
            if (e.status() == Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    void reset() {
        byVhost.clear();
    }

    private boolean stale(Listed listed) {
        Duration age = Duration.between(listed.at(), cluster.options().clock().instant());
        return age.compareTo(RabbitMqOptions.watchInterval(cluster.spec())) >= 0;
    }

    private Listed load(String vhost) {
        Deadline deadline = Deadlines.current(cluster.options());
        Map<String, KindId> kinds = new HashMap<>();
        int page = 1;
        int pages;
        do {
            JsonNode body = cluster.management()
                    .get(
                            "/api/queues/" + ManagementPaths.segment(vhost),
                            Map.of(
                                    "columns",
                                    "name,vhost,type",
                                    "disable_stats",
                                    "true",
                                    "page",
                                    Integer.toString(page),
                                    "page_size",
                                    Integer.toString(Listing.MAX_PAGE_SIZE)),
                            deadline,
                            "list queue types")
                    .body();
            JsonNode items = body.isArray() ? body : body.path("items");
            items.forEach(item -> kind(item.path("type").asString())
                    .ifPresent(k -> kinds.put(item.path("name").asString(), k)));
            pages = body.isArray() ? 1 : body.path("page_count").asInt(1);
            page++;
        } while (page <= pages);
        Listed listed = new Listed(Map.copyOf(kinds), cluster.options().clock().instant());
        byVhost.put(vhost, listed);
        return listed;
    }

    private static Optional<KindId> kind(String type) {
        return switch (type) {
            case "classic" -> Optional.of(RabbitMqKinds.CLASSIC_QUEUE);
            case "quorum" -> Optional.of(RabbitMqKinds.QUORUM_QUEUE);
            case "stream" -> Optional.of(RabbitMqKinds.STREAM);
            default -> Optional.empty();
        };
    }
}
