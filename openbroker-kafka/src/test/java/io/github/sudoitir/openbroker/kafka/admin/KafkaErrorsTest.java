package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

class KafkaErrorsTest {

    @Test
    void rejectedCredentialsAreUnauthenticatedAndNeverSent() {
        BrokerException e = KafkaErrors.of(new SaslAuthenticationException("bad"), null);

        assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }

    @Test
    void aTlsFailureIsNotACredentialFailure() {
        assertThat(KafkaErrors.of(new SslAuthenticationException("handshake"), null)
                        .reason())
                .isEqualTo(ErrorReasons.TLS_HANDSHAKE_FAILED);
    }

    @Test
    void anAuthorizationFailureIsPermissionDenied() {
        BrokerException e = KafkaErrors.of(new GroupAuthorizationException("no"), null);

        assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_AUTHORIZED);
    }

    @Test
    void missingAndExistingResourcesMapToTheirReasons() {
        assertThat(KafkaErrors.of(new UnknownTopicOrPartitionException("t"), null)
                        .reason())
                .isEqualTo(ErrorReasons.RESOURCE_GONE);
        assertThat(KafkaErrors.of(new TopicExistsException("t"), null).status()).isEqualTo(Status.ALREADY_EXISTS);
    }

    @Test
    void aTimeoutIsUnreachableAndInDoubt() {
        BrokerException e = KafkaErrors.of(new TimeoutException("slow"), null);

        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void aConfigurationErrorQuotesNothingOfTheSettings() {
        BrokerException e = KafkaErrors.of(new ConfigException("sasl.jaas.config", "password=hunter2"), null);

        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(e.getMessage()).doesNotContain("hunter2");
        assertThat(e.getCause()).isNull();
        assertThat(e.info().metadata()).isEmpty();
    }
}
