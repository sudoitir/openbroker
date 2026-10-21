package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.MessagingTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The messaging TCK against one Artemis broker of the image the build names ({@code artemis.image}).
 * The broker is {@link TestBroker}: the image's defaults plus an account that may use {@code tck.*}
 * but not {@code tck.denied.*}, and a TLS acceptor that requires a client certificate.
 */
@Testcontainers
class ArtemisMessagingTckIT extends MessagingTck {

    @Container
    static final GenericContainer<?> BROKER = TestBroker.container();

    @Override
    protected TckEnvironment environment() {
        return new TckEnvironment() {
            @Override
            public BrokerDriver driver() {
                return new ArtemisDriver();
            }

            @Override
            public ConnectRequest request() {
                return TestBroker.request(BROKER, TestBroker.USER, TestBroker.PASSWORD);
            }

            @Override
            public ConnectRequest requestWithWrongPassword(String wrongPassword) {
                return TestBroker.request(BROKER, TestBroker.USER, wrongPassword);
            }

            @Override
            public Optional<ConnectRequest> restrictedRequest() {
                return Optional.of(
                        TestBroker.request(BROKER, TestBroker.RESTRICTED_USER, TestBroker.RESTRICTED_PASSWORD));
            }

            @Override
            public Optional<ConnectRequest> mutualTlsRequest() {
                return Optional.of(TestBroker.mutualTls(BROKER));
            }

            @Override
            public KindId subscriptionKind() {
                return ArtemisKinds.QUEUE;
            }

            @Override
            public ResourceId subscriptionId(String name) {
                return new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of(name));
            }

            @Override
            public Attributes subscriptionAttributes(String name) {
                return Attributes.builder()
                        .put(StandardAttributes.DURABLE, true)
                        .build();
            }

            @Override
            public MessageFilter nativeEquals(String name, String value) {
                return ArtemisFilters.selector(name + " = '" + value.replace("'", "''") + "'");
            }

            @Override
            public String nativeDialect() {
                return ArtemisFilters.DIALECT;
            }
        };
    }
}
