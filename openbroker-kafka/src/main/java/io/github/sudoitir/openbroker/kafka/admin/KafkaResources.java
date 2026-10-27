package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.ResourcePage;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Resources;
import java.util.List;

/** The Kafka kinds and their resources. */
public final class KafkaResources implements Resources {

    private final KafkaCluster cluster;

    public KafkaResources(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Partial<List<ResourcePage>> read(List<ResourceQuery> queries) {
        throw KafkaErrors.unimplemented("read resources yet");
    }

    @Override
    public List<Kind> kinds() {
        return List.of();
    }
}
