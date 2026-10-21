package io.github.sudoitir.openbroker.artemis;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.Operation;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Rotated TLS material reaches the links opened after it and leaves the open ones alone: a link
 * opened with the old material keeps working, and a link opened with the new material works beside
 * it.
 */
@Testcontainers
class ArtemisRotationIT {

    @Container
    static final GenericContainer<?> BROKER = TestBroker.container();

    @Test
    void rotatingTlsMaterialLeavesOpenLinksAlone() {
        AtomicReference<TlsMaterial> material =
                new AtomicReference<>(TestBroker.clientMaterial(BROKER, "client-before-rotation"));
        TlsProvider provider = () -> Optional.of(material.get());
        ResourceId queue = new ResourceId(
                ArtemisKinds.QUEUE,
                ResourcePath.of("tck.rotation." + UUID.randomUUID().toString().substring(0, 8)));
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(new ArtemisDriver()));
                ClusterConnection admin =
                        drivers.connect(TestBroker.request(BROKER, TestBroker.USER, TestBroker.PASSWORD));
                ClusterConnection secure = drivers.connect(TestBroker.mutualTls(BROKER, provider))) {
            admin.operations()
                    .execute(
                            queue,
                            new Operation.Create(Attributes.builder()
                                    .put(StandardAttributes.DURABLE, true)
                                    .build()),
                            Scope.whole())
                    .orThrow();
            try {
                Transport transport = secure.transport().orElseThrow();
                try (Sender before =
                        transport.sender(SendTarget.subscription(queue), Scope.whole(), SenderOptions.defaults())) {
                    before.send(message("before"));

                    material.set(TestBroker.clientMaterial(BROKER, "client-after-rotation"));

                    try (Sender after =
                            transport.sender(SendTarget.subscription(queue), Scope.whole(), SenderOptions.defaults())) {
                        after.send(message("after"));
                        assertThat(after.state()).isEqualTo(LinkState.OPEN);
                    }
                    before.send(message("before again"));
                    assertThat(before.state())
                            .as("the link opened with the old material")
                            .isEqualTo(LinkState.OPEN);
                }
                try (Receiver receiver = transport.receiver(queue, Scope.whole(), ReceiverOptions.consume())) {
                    List<String> bodies = new ArrayList<>();
                    long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (bodies.size() < 3 && System.nanoTime() < until) {
                        for (Message m : receiver.receive(3, Duration.ofSeconds(1))) {
                            bodies.add(
                                    new String(m.body().bytes().orElseThrow().toByteArray(), StandardCharsets.UTF_8));
                            receiver.settle(m, Disposition.accept());
                        }
                    }
                    assertThat(bodies).containsExactlyInAnyOrder("before", "after", "before again");
                }
            } finally {
                admin.operations().execute(queue, new Operation.Delete(false, true), Scope.whole());
            }
        }
    }

    private static OutboundMessage message(String body) {
        return OutboundMessage.of(MessageBody.of(body.getBytes(StandardCharsets.UTF_8)));
    }
}
