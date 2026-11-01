package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.ClusterMember;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.List;

/** {@link Nodes} from {@code /api/nodes} and the listeners of {@code /api/overview}; the node ids are the native node names. */
final class RabbitMqNodes implements Nodes {

    RabbitMqNodes(RabbitMqCluster cluster) {}

    @Override
    public Partial<List<NodeDescription>> describe(Scope scope) {
        throw RabbitMqErrors.unimplemented("RabbitMqNodes.describe");
    }

    @Override
    public Partial<List<ClusterMember>> members(Scope scope) {
        throw RabbitMqErrors.unimplemented("RabbitMqNodes.members");
    }
}
