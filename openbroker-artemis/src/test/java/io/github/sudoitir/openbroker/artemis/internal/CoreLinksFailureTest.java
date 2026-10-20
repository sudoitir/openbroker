package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.junit.jupiter.api.Test;

/** A Core refusal becomes the SDK's status and reason, so a caller tells them apart without Artemis types. */
class CoreLinksFailureTest {

    private static final NodeId NODE = NodeId.of("n1");

    @Test
    void aQueueThatAdmitsNoMoreConsumersIsMaxConsumersReached() {
        BrokerException e = CoreLinks.failure(NODE, new ActiveMQQueueMaxConsumerLimitReached("max 1"));

        assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.MAX_CONSUMERS_REACHED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
    }

    @Test
    void aFullAddressIsDestinationFull() {
        BrokerException e = CoreLinks.failure(NODE, new ActiveMQAddressFullException("full"));

        assertThat(e.reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
    }
}
