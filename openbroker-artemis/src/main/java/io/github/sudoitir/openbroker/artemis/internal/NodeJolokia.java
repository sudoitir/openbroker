package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisJolokia;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** A node's Jolokia agent with the deadline of the call it serves. */
public final class NodeJolokia implements ArtemisJolokia {

    private final JolokiaClient client;
    private final Supplier<Deadline> deadline;

    public NodeJolokia(JolokiaClient client, Supplier<Deadline> deadline) {
        this.client = client;
        this.deadline = deadline;
    }

    @Override
    public NodeId node() {
        return client.node();
    }

    @Override
    public String brokerName() {
        return client.brokerName(deadline.get());
    }

    @Override
    public JolokiaResponse single(JolokiaRequest request) {
        return client.single(request, deadline.get(), Optional.empty());
    }

    @Override
    public List<JolokiaResponse> batch(List<JolokiaRequest> requests) {
        return client.batch(requests, deadline.get(), Optional.empty());
    }

    @Override
    public List<String> search(String pattern) {
        return client.search(pattern, deadline.get());
    }

    @Override
    public JolokiaResponse readBroker(String... attributes) {
        return client.readBrokerAttributes(deadline.get(), attributes);
    }

    @Override
    public JolokiaResponse execOnBroker(String operation, Object... arguments) {
        return client.execOnBroker(deadline.get(), Optional.empty(), operation, arguments);
    }

    @Override
    public @Nullable JsonNode execOnBrokerParsed(String operation, Object... arguments) {
        return client.execOnBrokerParsed(deadline.get(), operation, arguments);
    }

    @Override
    public @Nullable JsonNode parsed(JolokiaResponse entry) {
        return client.parsed(entry);
    }

    @Override
    public ObjectMapper mapper() {
        return client.mapper();
    }
}
