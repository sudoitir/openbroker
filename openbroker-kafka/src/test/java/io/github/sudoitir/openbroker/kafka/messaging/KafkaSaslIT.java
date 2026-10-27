package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The messaging client against a broker that requires a login and authorizes by account. */
class KafkaSaslIT {

    private static SaslKafka kafka;
    private static BrokerDrivers drivers;

    @BeforeAll
    static void start() {
        kafka = SaslKafka.start();
        drivers = BrokerDrivers.load();
    }

    @AfterAll
    static void stop() {
        drivers.close();
        kafka.close();
    }

    private static ClusterConnection connect(String mechanism, CredentialSource credentials) {
        return drivers.connect(
                new ConnectRequest(KafkaDriver.SYSTEM, kafka.spec(mechanism), credentials, ConnectOptions.defaults()));
    }

    private static String topic(String name) {
        return "t." + name + "." + UUID.randomUUID().toString().substring(0, 8);
    }

    private static ResourceId group(String topic) {
        return SharedKafka.group(topic);
    }

    private static void await(String what, BooleanSupplier condition) {
        long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static void createTopic(String name) {
        try (Admin admin = kafka.admin()) {
            admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void plainAuthenticatesAndMovesMessages() {
        String topic = topic("plain");
        createTopic(topic);
        ResourceId subscription = group(topic);
        try (ClusterConnection connection = connect(
                KafkaOptions.PLAIN,
                CredentialSource.of(Credentials.usernamePassword(SaslKafka.APP, SaslKafka.APP_PASSWORD)))) {
            Transport transport = connection.transport().orElseThrow();
            try (Sender sender =
                    transport.sender(SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults())) {
                sender.send(KafkaMessagingIT.message(7));
            }
            try (Receiver receiver = transport.receiver(subscription, Scope.whole(), ReceiverOptions.consume())) {
                List<Message> got = new ArrayList<>();
                await("the message", () -> {
                    got.addAll(receiver.receive(1, Duration.ofMillis(300)));
                    return !got.isEmpty();
                });
                assertThat(KafkaMessagingIT.seq(got.getFirst())).isEqualTo(7);
                receiver.settle(got.getFirst(), Disposition.accept());
            }
        }
    }

    @Test
    void aWrongPasswordIsRefusedAsRejectedCredentials() {
        String topic = topic("wrong");
        createTopic(topic);
        try (ClusterConnection connection =
                connect(KafkaOptions.PLAIN, CredentialSource.of(Credentials.usernamePassword(SaslKafka.APP, "nope")))) {
            Transport transport = connection.transport().orElseThrow();
            assertThatThrownBy(() -> transport.sender(
                            SendTarget.subscription(group(topic)), Scope.whole(), SenderOptions.defaults()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED);
                        assertThat(e.getMessage()).doesNotContain("nope");
                    });
        }
    }

    @Test
    void anAccountThatMayNotUseATopicIsDeniedNamingItAndTheClientKeepsWorking() {
        String denied = topic("denied");
        String allowed = "t.ok." + UUID.randomUUID().toString().substring(0, 8);
        createTopic(denied);
        createTopic(allowed);
        try (ClusterConnection connection = connect(
                KafkaOptions.PLAIN,
                CredentialSource.of(Credentials.usernamePassword(SaslKafka.LIMITED, SaslKafka.LIMITED_PASSWORD)))) {
            Transport transport = connection.transport().orElseThrow();
            assertThatThrownBy(() -> transport.sender(
                            SendTarget.subscription(group(denied)), Scope.whole(), SenderOptions.defaults()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_AUTHORIZED);
                        assertThat(e.info().metadata().get(ErrorReasons.RESOURCE))
                                .contains(denied);
                    });
            try (Sender sender = transport.sender(
                    SendTarget.subscription(group(allowed)), Scope.whole(), SenderOptions.defaults())) {
                assertThat(sender.send(KafkaMessagingIT.message(1)).ref()).isPresent();
            }
        }
    }

    @Test
    void aRotatedScramPasswordReachesTheNextAuthenticationWhileAProcessorRuns() {
        String topic = topic("rotate");
        createTopic(topic);
        kafka.scramPassword("rotating", "first");
        AtomicReference<String> password = new AtomicReference<>("first");
        ResourceId subscription = group(topic);
        AtomicInteger handled = new AtomicInteger();
        kafka.grant("rotating");
        try (ClusterConnection connection = connect(
                KafkaOptions.SCRAM_SHA_256, plane -> Credentials.usernamePassword("rotating", password.get()))) {
            Transport transport = connection.transport().orElseThrow();
            try (Processor processor = transport.process(
                            subscription,
                            Scope.whole(),
                            ProcessorOptions.defaults().withConcurrency(1),
                            ctx -> {
                                handled.incrementAndGet();
                                return Disposition.accept();
                            });
                    Sender sender = transport.sender(
                            SendTarget.subscription(subscription), Scope.whole(), SenderOptions.defaults())) {
                sender.send(KafkaMessagingIT.message(1));
                await("the first message", () -> handled.get() == 1);

                kafka.scramPassword("rotating", "second");
                password.set("second");
                try {
                    Thread.sleep(SaslKafka.REAUTH_MS * 3L);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

                sender.send(KafkaMessagingIT.message(2));
                await("a message after the rotation", () -> handled.get() == 2);
            }
        }
    }

    @Test
    void aDefaultProcessorWhoseDeadLetterTopicCannotBeMadeIsRefusedBeforeTheGroupIsJoined() throws Exception {
        String topic = topic("locked");
        createTopic(topic);
        kafka.grantOnly(SaslKafka.LOCKED, topic);
        ResourceId subscription = group(topic);
        try (ClusterConnection connection = connect(
                KafkaOptions.PLAIN,
                CredentialSource.of(Credentials.usernamePassword(SaslKafka.LOCKED, SaslKafka.LOCKED_PASSWORD)))) {
            Transport transport = connection.transport().orElseThrow();
            assertThatThrownBy(() -> transport.process(
                            subscription, Scope.whole(), ProcessorOptions.defaults(), ctx -> Disposition.accept()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.getMessage()).contains(topic + ".dlt").contains("create it");
                    });
        }
        try (Admin admin = kafka.admin()) {
            assertThat(admin.listConsumerGroups().all().get())
                    .extracting(listing -> listing.groupId())
                    .doesNotContain(subscription.path().segments().getFirst());
        }
    }
}
