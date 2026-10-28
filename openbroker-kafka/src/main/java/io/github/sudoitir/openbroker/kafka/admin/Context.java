package io.github.sudoitir.openbroker.kafka.admin;

import java.time.Instant;
import org.apache.kafka.clients.admin.Admin;
import org.jspecify.annotations.Nullable;

/**
 * What a handler needs for one call: the connection, the Admin client, the deadline and the time.
 * {@code reads} is set when the call reads several queries, which then share their rounds.
 */
record Context(
        KafkaCluster cluster,
        Topology topology,
        Admin admin,
        Await await,
        Instant now,
        @Nullable Reads reads) {}
