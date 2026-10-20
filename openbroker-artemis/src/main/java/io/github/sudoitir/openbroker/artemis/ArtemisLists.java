package io.github.sudoitir.openbroker.artemis;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Artemis' {@code listX} management operations and batched MBean reads.
 *
 * <p>The list operations behind every listing. Each {@code listX(String,int,int)} returns a JSON-encoded
 * {@code {"data":[…],"count":N}} string, and every scalar in a row is a quoted string.
 */
public final class ArtemisLists {

    /**
     * The filter that matches every row. An empty string means the same on current brokers, but older
     * ones (2.33.0 among them) parse the options as JSON and fail on it.
     */
    public static final String ALL = "{\"field\":\"\",\"operation\":\"\",\"value\":\"\"}";

    /**
     * The page and page size that read every row: the first page of an unbounded one. A page and size of
     * -1 mean the same on current brokers, but older ones (2.33.0 among them) answer them with no rows.
     */
    public static final int FIRST_PAGE = 1;

    public static final int EVERY_ROW = Integer.MAX_VALUE;

    /** How many MBeans one {@link #readAll} request reads. */
    public static final int BATCH = 200;

    private ArtemisLists() {}

    /** One page of one op on one node. */
    public record Page(@Nullable JsonNode data, long count) {}

    public static Page fetch(ArtemisJolokia client, String op, String options, int page, int size) {
        JsonNode env = client.execOnBrokerParsed(op + "(java.lang.String,int,int)", options, page, size);
        JsonNode data = env == null ? null : env.get("data");
        int fallback = data != null && data.isArray() ? data.size() : 0;
        long count = env == null ? 0L : env.path("count").asLong(fallback);
        return new Page(data, count);
    }

    /**
     * Reads every named MBean in full, in batches of {@link #BATCH}, each request with its own permit.
     * An MBean that disappeared between a search and this read contributes nothing.
     */
    public static <T> List<T> readAll(ArtemisJolokia client, List<String> objectNames, Function<JsonNode, T> parse) {
        if (objectNames.isEmpty()) {
            return List.of();
        }
        int size = objectNames.size();
        return IntStream.range(0, (size + BATCH - 1) / BATCH)
                .mapToObj(i -> objectNames.subList(i * BATCH, Math.min(size, (i + 1) * BATCH)))
                .flatMap(chunk ->
                        client.batch(chunk.stream().map(JolokiaRequest::readAll).toList()).stream())
                .filter(r -> r.ok() && r.value() != null && r.value().isObject())
                .map(r -> parse.apply(r.value()))
                .toList();
    }

    public static long num(@Nullable JsonNode row, String field) {
        JsonNode v = row == null ? null : row.get(field);
        if (v == null || v.isNull()) {
            return 0L;
        }
        if (v.isNumber()) {
            return v.asLong();
        }
        try {
            return Long.parseLong(v.asString().trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public static boolean flag(@Nullable JsonNode row, String field) {
        JsonNode v = row == null ? null : row.get(field);
        return v != null && !v.isNull() && (v.isBoolean() ? v.asBoolean() : Boolean.parseBoolean(v.asString()));
    }

    public static @Nullable String str(@Nullable JsonNode row, String field) {
        JsonNode v = row == null ? null : row.get(field);
        return v == null || v.isNull() ? null : v.asString();
    }
}
