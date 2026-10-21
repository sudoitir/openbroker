package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import java.util.Optional;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
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

    @Test
    void aRolledBackTransactionIsAbortedAndRejected() {
        BrokerException e = CoreLinks.failure(NODE, new ActiveMQTransactionRolledBackException("rolled back"));

        assertThat(e.status()).isEqualTo(Status.ABORTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.TRANSACTION_ROLLED_BACK);
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
    }

    @Test
    void aRefusalNamesThePlaneAndTheResource() {
        ResourceId queue = new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of("tck.denied.x"));

        BrokerException e = CoreLinks.failure(NODE, new ActiveMQSecurityException("no"), Optional.of(queue));

        assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_AUTHORIZED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
        assertThat(e.info().metadata())
                .containsEntry(ErrorReasons.PLANE, "DATA")
                .containsEntry(ErrorReasons.RESOURCE, queue.format());
    }

    @Test
    void whatArtemisCannotDoIsUnimplementedAndNotSupported() {
        BrokerException e = Unimplemented.of("route by partition");

        assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }
}
