package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.activemq.ArtemisContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * A Spring Boot application on a real Artemis broker, with no {@code openbroker.*} key: Spring's service
 * connection is all the configuration there is. The application sends, processes, reports its health and
 * shuts down while a handler is still running.
 */
@SpringBootTest(
        classes = SpringBootArtemisIT.Application.class,
        properties = "spring.autoconfigure.exclude=org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpringBootArtemisIT {

    @Container
    @ServiceConnection
    static final ArtemisContainer ARTEMIS =
            new ArtemisContainer(DockerImageName.parse(System.getProperty("artemis.image", "apache/artemis:2.57.0"))
                    .asCompatibleSubstituteFor("apache/activemq-artemis"));

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
    ConnectionFactory connectionFactory;

    @Autowired
    OpenBrokerProcessors processors;

    @Autowired
    CompositeHealthContributor openbrokerHealthContributor;

    @Test
    @Order(2)
    @DirtiesContext
    void sendsProcessesReportsHealthAndShutsDownGracefully() throws Exception {
        String name = "orders-" + UUID.randomUUID();
        // The broker creates a queue (as ANYCAST) for the first JMS producer; OpenBroker's own send then finds it.
        try (JMSContext jms = connectionFactory.createContext()) {
            jms.createProducer().send(jms.createQueue(name), jms.createMessage());
        }
        try (Sender sender = transport.sender(
                SendTarget.destination(ResourceId.of("artemis.address", name)),
                Scope.whole(),
                SenderOptions.defaults())) {
            sender.send(new OutboundMessage(
                    MessageHeaders.builder().build(),
                    Map.of(),
                    MessageBody.of("hello".getBytes(StandardCharsets.UTF_8)),
                    Map.of()));
        }

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        processors.register("artemis", ResourceId.of("artemis.queue", name), Scope.whole(), message -> {
            if (message.message().body().bytes().orElseThrow().asUtf8().equals("hello")) {
                entered.countDown();
                release.await();
                finished.set(true);
            }
            return Disposition.accept();
        });
        assertThat(entered.await(30, TimeUnit.SECONDS))
                .as("the handler ran for the sent message")
                .isTrue();

        Health health = ((HealthIndicator) openbrokerHealthContributor.getContributor("artemis")).health();
        // Spring's details state no management endpoint, so the cluster is neither shown up nor down.
        assertThat(health.getStatus().getCode()).isEqualTo("UNKNOWN");

        Thread closing = Thread.ofPlatform().start(context::stop);
        closing.join(Duration.ofSeconds(1));
        assertThat(closing.isAlive())
                .as("stopping waits for the handler within its grace")
                .isTrue();
        release.countDown();
        closing.join(Duration.ofSeconds(30));
        assertThat(closing.isAlive()).isFalse();
        assertThat(finished).isTrue();
    }

    @Test
    @Order(1)
    void theManagementFacetsReportUnavailableWithAReason() {
        var described = connection.nodes().describe(Scope.whole());

        assertThat(described.failures()).isNotEmpty();
        assertThat(described.failures().values())
                .allSatisfy(failure -> assertThat(failure.reason()).isNotBlank());
    }
}
