package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A Spring Boot application on a real Kafka broker, with no {@code openbroker.*} key: Spring's service
 * connection is all the configuration there is. The application sends, processes, reports its health and
 * stops while a handler is still running.
 */
@SpringBootTest(
        classes = SpringBootKafkaIT.Application.class,
        properties =
                "spring.autoconfigure.exclude=org.springframework.boot.artemis.autoconfigure.ArtemisAutoConfiguration")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpringBootKafkaIT {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(System.getProperty("kafka.image", "apache/kafka:4.4.0")));

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Application {}

    @Autowired
    ConfigurableApplicationContext context;

    @Autowired
    ClusterConnection connection;

    @Autowired
    Transport transport;

    @Autowired
    OpenBrokerProcessors processors;

    @Autowired
    CompositeHealthContributor openbrokerHealthContributor;

    @Test
    @Order(1)
    void theClusterAnswersAndHealthIsUp() {
        HealthIndicator indicator = (HealthIndicator) openbrokerHealthContributor.getContributor("kafka");
        // The first check opens the admin client and may pass the deadline: unknown, never down.
        List<String> seen = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    seen.add(indicator.health().getStatus().getCode());
                    return seen.getLast().equals("UP");
                });

        assertThat(seen).doesNotContain("DOWN");
    }

    @Test
    @Order(2)
    @DirtiesContext
    void sendsProcessesAndStopsGracefully() throws Exception {
        String topic = "orders-" + UUID.randomUUID();
        ResourceId topicId = ResourceId.of("kafka.topic", topic);
        connection
                .operations()
                .execute(topicId, new Operation.Create(Attributes.builder().build()), Scope.whole())
                .orThrow();
        try (Sender sender =
                transport.sender(SendTarget.destination(topicId), Scope.whole(), SenderOptions.defaults())) {
            sender.send(new OutboundMessage(
                    MessageHeaders.builder().build(),
                    Map.of(),
                    MessageBody.of("hello".getBytes(StandardCharsets.UTF_8)),
                    Map.of()));
        }

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        processors.register(
                "kafka", ResourceId.of("kafka.group-topic", "orders-group", topic), Scope.whole(), message -> {
                    entered.countDown();
                    release.await();
                    finished.set(true);
                    return Disposition.accept();
                });
        assertThat(entered.await(60, TimeUnit.SECONDS))
                .as("the handler ran for the sent message")
                .isTrue();

        Thread stopping = Thread.ofPlatform().start(context::stop);
        stopping.join(Duration.ofSeconds(1));
        assertThat(stopping.isAlive())
                .as("stopping waits for the handler within its grace")
                .isTrue();
        release.countDown();
        stopping.join(Duration.ofSeconds(30));
        assertThat(stopping.isAlive()).isFalse();
        assertThat(finished).isTrue();
    }
}
