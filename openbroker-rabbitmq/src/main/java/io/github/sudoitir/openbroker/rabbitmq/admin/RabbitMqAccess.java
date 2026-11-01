package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Optional;

/** {@link Access}: what the account may do, from its user tags, its permissions and the enabled plugins. */
final class RabbitMqAccess implements Access {

    RabbitMqAccess(RabbitMqCluster cluster) {}

    @Override
    public Assessment check(Capability capability, Optional<ResourceId> resource) {
        throw RabbitMqErrors.unimplemented("RabbitMqAccess.check");
    }

    @Override
    public AccessSnapshot probe() {
        throw RabbitMqErrors.unimplemented("RabbitMqAccess.probe");
    }
}
