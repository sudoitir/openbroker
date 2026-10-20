package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.NodeId;
import java.util.List;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * One node's Jolokia agent, for what has no neutral meaning. Every request asks the connection's
 * interceptor for a permit, honours the caller's deadline, and fails with a {@code BrokerException}
 * when the call does not get an answer; a {@link JolokiaResponse} that is not {@link
 * JolokiaResponse#ok()} is the broker's own refusal, for the caller to read.
 */
public interface ArtemisJolokia {

    NodeId node();

    /** The broker's top-level MBean name, resolved once and cached for the endpoint. */
    String brokerName();

    /** Sends one request. */
    JolokiaResponse single(JolokiaRequest request);

    /** Sends several requests in one POST; the result is aligned with {@code requests}. */
    List<JolokiaResponse> batch(List<JolokiaRequest> requests);

    /** MBean names matching a JMX pattern. */
    List<String> search(String pattern);

    /** Reads attributes of the broker MBean in one request. */
    JolokiaResponse readBroker(String... attributes);

    /** Invokes an operation on the broker MBean. */
    JolokiaResponse execOnBroker(String operation, Object... arguments);

    /** A broker operation whose value is a JSON string, parsed; fails when the broker refused it. */
    @Nullable JsonNode execOnBrokerParsed(String operation, Object... arguments);

    /** The value of a batch entry, parsed again when it is a JSON string; fails when the entry failed. */
    @Nullable JsonNode parsed(JolokiaResponse entry);

    /** The mapper responses are read with, for values the caller parses itself. */
    ObjectMapper mapper();
}
