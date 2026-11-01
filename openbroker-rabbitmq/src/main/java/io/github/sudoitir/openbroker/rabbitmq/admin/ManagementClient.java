package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.Deadline;
import java.util.Map;
import java.util.Optional;
import tools.jackson.databind.JsonNode;

/**
 * The management API client: JSON through Jackson 3, preemptive authentication on every request, a permit from the
 * call interceptor, the request timeout set to the time left on the deadline, the body capped, and endpoint failover
 * after a request that was not sent. It never logs a body or a credential.
 */
public final class ManagementClient {

    /** A response: its JSON body ({@code MissingNode} when empty) and the {@code Location} header when present. */
    public record Response(JsonNode body, Optional<String> location) {}

    ManagementClient(RabbitMqCluster cluster) {}

    public Response get(String path, Map<String, String> query, Deadline deadline, String operation) {
        throw RabbitMqErrors.unimplemented("ManagementClient.get");
    }

    public Response put(String path, JsonNode body, Deadline deadline, String operation) {
        throw RabbitMqErrors.unimplemented("ManagementClient.put");
    }

    public Response post(String path, JsonNode body, Deadline deadline, String operation) {
        throw RabbitMqErrors.unimplemented("ManagementClient.post");
    }

    public Response delete(String path, Map<String, String> query, Deadline deadline, String operation) {
        throw RabbitMqErrors.unimplemented("ManagementClient.delete");
    }
}
