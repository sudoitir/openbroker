package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.tck.OpenBrokerTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;

/** The TCK against a three-broker cluster of the image the build names ({@code kafka.image}). */
class KafkaTckIT extends OpenBrokerTck {

    @Override
    protected TckEnvironment environment() {
        return new KafkaTckEnvironment(() -> new ConnectRequest(
                KafkaDriver.SYSTEM,
                KafkaTestCluster.shared().spec(),
                CredentialSource.none(),
                ConnectOptions.defaults()));
    }
}
