package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.MessagingTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;

/**
 * The messaging TCK against the three-broker cluster, on consumer-group subscriptions. A subscription
 * is a group on a topic; creating one through the connection makes the topic. Share groups have no
 * create operation to make a subscription with, so their cases are in {@code KafkaShareIT}.
 */
class KafkaMessagingTckIT extends MessagingTck {

    @Override
    protected TckEnvironment environment() {
        return new TckEnvironment() {
            @Override
            public BrokerDriver driver() {
                return new KafkaDriver();
            }

            @Override
            public ConnectRequest request() {
                return new ConnectRequest(
                        KafkaDriver.SYSTEM,
                        KafkaTestCluster.shared().spec(),
                        CredentialSource.none(),
                        ConnectOptions.defaults());
            }

            @Override
            public ConnectRequest requestWithWrongPassword(String wrongPassword) {
                return request();
            }

            @Override
            public KindId subscriptionKind() {
                return KafkaKinds.GROUP_TOPIC;
            }

            @Override
            public ResourceId subscriptionId(String name) {
                return new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(name + "-group", name));
            }

            @Override
            public Attributes subscriptionAttributes(String name) {
                return Attributes.empty();
            }

            @Override
            public boolean competingReceivers() {
                return false;
            }

            @Override
            public MessageFilter nativeEquals(String name, String value) {
                return new MessageFilter(nativeDialect(), name + "=" + value);
            }

            @Override
            public String nativeDialect() {
                return "kafka.none";
            }
        };
    }
}
