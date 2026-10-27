package io.github.sudoitir.openbroker.kafka.admin;

import java.time.Instant;
import org.apache.kafka.clients.admin.Admin;

/** What a handler needs for one call: the connection, the Admin client, the deadline and the time. */
record Context(KafkaCluster cluster, Topology topology, Admin admin, Await await, Instant now) {}
