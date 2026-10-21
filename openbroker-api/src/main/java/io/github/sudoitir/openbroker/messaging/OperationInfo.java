package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * What an observed operation is.
 *
 * @param messagingSystem the system's name, such as {@code artemis}
 * @param type the kind of operation
 * @param operation the operation's name
 * @param destination the destination it works on
 * @param subscription the subscription, when it is on one
 * @param node the node it ran on
 * @param serverAddress the node's host, when known
 * @param serverPort the node's port, when known
 * @param messageCount how many messages it covers
 */
public record OperationInfo(
        String messagingSystem,
        OperationType type,
        String operation,
        ResourceId destination,
        Optional<ResourceId> subscription,
        NodeId node,
        Optional<String> serverAddress,
        OptionalInt serverPort,
        int messageCount) {

    public OperationInfo {
        Objects.requireNonNull(messagingSystem, "messagingSystem");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(subscription, "subscription");
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(serverAddress, "serverAddress");
        Objects.requireNonNull(serverPort, "serverPort");
    }
}
