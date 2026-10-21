package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.NodeId;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A link changed state.
 *
 * @param link the link's id
 * @param node the node the link was opened on
 * @param state the new state
 * @param servedBy the node now serving it, when that is not {@code node}
 * @param cause why it left {@code OPEN}
 * @param at when
 */
public record LinkEvent(
        String link,
        NodeId node,
        LinkState state,
        Optional<NodeId> servedBy,
        Optional<BrokerException> cause,
        Instant at) {

    public LinkEvent {
        Objects.requireNonNull(link, "link");
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(servedBy, "servedBy");
        Objects.requireNonNull(cause, "cause");
        Objects.requireNonNull(at, "at");
    }
}
