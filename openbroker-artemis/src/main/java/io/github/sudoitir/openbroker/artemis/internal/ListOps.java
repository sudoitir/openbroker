package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * The list operations behind every listing. Each {@code listX(String,int,int)} returns a JSON-encoded
 * {@code {"data":[…],"count":N}} string, and every scalar in a row is a quoted string.
 */
public final class ListOps {

    /**
     * The filter that matches every row. An empty string means the same on current brokers, but older
     * ones (2.33.0 among them) parse the options as JSON and fail on it.
     */
    public static final String ALL = "{\"field\":\"\",\"operation\":\"\",\"value\":\"\"}";

    /** How many MBeans one {@link #readAll} request reads. */
    public static final int BATCH = 200;

    private ListOps() {}

    /** One page of one op on one node; {@code page} and {@code size} of -1 disable paging. */
    public record Page(@Nullable JsonNode data, long count) {}

    public static Page fetch(JolokiaClient client, Deadline deadline, String op, String options, int page, int size) {
        JsonNode env = client.execOnBrokerParsed(deadline, op + "(java.lang.String,int,int)", options, page, size);
        JsonNode data = env == null ? null : env.get("data");
        int fallback = data != null && data.isArray() ? data.size() : 0;
        long count = env == null ? 0L : env.path("count").asLong(fallback);
        return new Page(data, count);
    }

    /**
     * Reads every named MBean in full, in batches of {@link #BATCH}, each request with its own permit.
     * An MBean that disappeared between a search and this read contributes nothing.
     */
    public static <T> List<T> readAll(
            JolokiaClient client, Deadline deadline, List<String> objectNames, Function<JsonNode, T> parse) {
        if (objectNames.isEmpty()) {
            return List.of();
        }
        int size = objectNames.size();
        return IntStream.range(0, (size + BATCH - 1) / BATCH)
                .mapToObj(i -> objectNames.subList(i * BATCH, Math.min(size, (i + 1) * BATCH)))
                .flatMap(chunk -> client.batch(chunk.stream().map(JolokiaRequest::readAll).toList(), deadline,
                                Optional.empty())
                        .stream())
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
