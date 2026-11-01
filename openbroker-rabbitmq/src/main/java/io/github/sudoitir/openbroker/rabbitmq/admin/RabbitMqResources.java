package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import java.util.List;

/**
 * {@link Resources}: dispatches a query by kind or class, validates its orderings and field selector, runs one
 * listing per kind and merges the pages.
 */
final class RabbitMqResources implements Resources {

    RabbitMqResources(RabbitMqCluster cluster) {}

    @Override
    public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
        throw RabbitMqErrors.unimplemented("RabbitMqResources.read");
    }

    @Override
    public List<Kind> kinds() {
        throw RabbitMqErrors.unimplemented("RabbitMqResources.kinds");
    }
}
