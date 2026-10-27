package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.model.NodeDescription;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributors;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.event.ContextClosedEvent;

class HealthTest {

    private final FakeDriver driver = new FakeDriver();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
            .withBean(BrokerDrivers.class, () -> BrokerDrivers.of(List.of(driver)))
            .withPropertyValues("openbroker.connections.orders.system=fake", "openbroker.health.timeout=300ms");

    private static Partial<List<NodeDescription>> nodes(Map<String, BrokerException> failed, String... answered) {
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (String node : answered) {
            outcomes.put(NodeId.of(node), NodeOutcome.APPLIED);
        }
        failed.forEach((node, error) -> {
            outcomes.put(NodeId.of(node), NodeOutcome.FAILED);
            failures.put(NodeId.of(node), error);
        });
        return new Partial<>(List.of(), outcomes, failures);
    }

    private static BrokerException unreachable() {
        return new BrokerException(
                io.github.sudoitir.openbroker.Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.UNREACHABLE),
                Dispatch.NOT_SENT,
                null,
                "Cannot reach tcp://secret-host:61616 as user app");
    }

    private static HealthIndicator indicator(AssertableApplicationContext context, String name) {
        CompositeHealthContributor openbroker =
                context.getBean("openbrokerHealthContributor", CompositeHealthContributor.class);
        return (HealthIndicator) openbroker.getContributor(name);
    }

    @Test
    void aClusterThatAnswersIsUp() {
        driver.describe = () -> nodes(Map.of(), "n1", "n2");
        runner.run(context -> {
            Health health = indicator(context, "orders").health();

            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("nodes", 2).containsEntry("answered", 2);
        });
    }

    @Test
    void oneNodeOfThreeDownIsStillUpAndTheDetailsNameItWithoutEndpoints() {
        driver.describe = () -> nodes(Map.of("n3", unreachable()), "n1", "n2");
        runner.run(context -> {
            Health health = indicator(context, "orders").health();

            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("nodes", 3).containsEntry("answered", 2);
            assertThat(health.getDetails().get("failed").toString())
                    .contains("n3", "UNAVAILABLE", "UNREACHABLE")
                    .doesNotContain("secret-host")
                    .doesNotContain("app");
        });
    }

    @Test
    void noNodeAnsweringIsDownWithTheReason() {
        driver.describe = () -> nodes(Map.of("n1", unreachable(), "n2", unreachable()));
        runner.run(context -> {
            Health health = indicator(context, "orders").health();

            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("answered", 0);
            assertThat(health.getDetails().get("failed").toString()).contains("UNREACHABLE");
        });
    }

    @Test
    void aConnectionWithNoManagementEndpointIsUnknownNotDown() {
        BrokerException noEndpoint = new BrokerException(
                io.github.sudoitir.openbroker.Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.ENDPOINT_NOT_FOUND),
                Dispatch.NOT_SENT,
                null,
                "This node has no management endpoint.");
        driver.describe = () -> nodes(Map.of("artemis", noEndpoint));
        runner.run(context -> {
            Health health = indicator(context, "orders").health();

            assertThat(health.getStatus()).isEqualTo(Status.UNKNOWN);
            assertThat(health.getDetails()).containsEntry("reason", ErrorReasons.ENDPOINT_NOT_FOUND);
        });
    }

    @Test
    void aFailureOfTheCallIsDownWithTheReasonAndNoMessage() {
        driver.describe = () -> {
            throw unreachable();
        };
        runner.run(context -> {
            Health health = indicator(context, "orders").health();

            assertThat(health.getStatus()).isEqualTo(Status.DOWN);
            assertThat(health.getDetails())
                    .containsEntry("reason", "UNREACHABLE")
                    .containsEntry("status", "UNAVAILABLE");
            assertThat(health.getDetails().toString()).doesNotContain("secret-host");
        });
    }

    @Test
    void theFirstCheckPassingItsDeadlineIsUnknownAndTheResultReplacesIt() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        driver.describe = () -> {
            calls.incrementAndGet();
            await(release);
            return nodes(Map.of(), "n1");
        };
        try {
            runner.run(context -> {
                HealthIndicator indicator = indicator(context, "orders");
                long started = System.nanoTime();

                Health first = indicator.health();

                assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
                assertThat(first.getStatus()).isEqualTo(Status.UNKNOWN);
                assertThat(first.getDetails()).containsEntry("reason", "first check still running");

                Health second = indicator.health();

                assertThat(calls).hasValue(1);
                assertThat(second.getStatus()).isEqualTo(Status.UNKNOWN);

                release.countDown();
                Thread.sleep(200);

                assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
            });
        } finally {
            release.countDown();
        }
    }

    @Test
    void aLaterCheckPassingItsDeadlineIsDownAndStaleUntilALateResultReplacesIt() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        driver.describe = () -> {
            if (calls.incrementAndGet() > 1) {
                await(release);
            }
            return nodes(Map.of(), "n1");
        };
        try {
            runner.run(context -> {
                HealthIndicator indicator = indicator(context, "orders");
                assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

                Health timedOut = indicator.health();

                assertThat(timedOut.getStatus()).isEqualTo(Status.DOWN);
                assertThat(timedOut.getDetails()).containsEntry("reason", "TIMEOUT");

                Health stale = indicator.health();

                assertThat(calls).hasValue(2);
                assertThat(stale.getStatus()).isEqualTo(Status.DOWN);
                assertThat(stale.getDetails()).containsEntry("stale", true);

                release.countDown();
                Thread.sleep(200);

                // The late result was remembered before the next check was allowed, so it is not stale DOWN.
                Health late = indicator.health();
                assertThat(late.getStatus()).isEqualTo(Status.UP);
                assertThat(calls).hasValue(3);
            });
        } finally {
            release.countDown();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void oneConnectionDownLeavesTheOthersUp() {
        driver.describeOf.put(1, () -> nodes(Map.of("n1", unreachable())));
        driver.describeOf.put(2, () -> nodes(Map.of(), "n1"));
        runner.withPropertyValues("openbroker.connections.billing.system=fake").run(context -> {
            assertThat(indicator(context, "billing").health().getStatus()).isEqualTo(Status.DOWN);
            assertThat(indicator(context, "orders").health().getStatus()).isEqualTo(Status.UP);
        });
    }

    @Test
    void aConnectionThatIsClosingIsOutOfService() {
        runner.run(context -> {
            context.publishEvent(new ContextClosedEvent(context));

            assertThat(indicator(context, "orders").health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        });
    }

    @Test
    void aRuntimeConnectionDoesNotJoinTheHealth() {
        runner.run(context -> {
            context.getBean(BrokerDrivers.class)
                    .connect(new ConnectRequest(
                            FakeDriver.SYSTEM,
                            ClusterSpec.ofNodes(List.of()),
                            CredentialSource.none(),
                            ConnectOptions.defaults()));

            assertThat(context.getBean("openbrokerHealthContributor", CompositeHealthContributor.class).stream()
                            .map(HealthContributors.Entry::name))
                    .containsExactly("orders");
        });
    }

    @Test
    void withoutActuatorOrWhenTurnedOffThereIsNoHealth() {
        runner.withClassLoader(new FilteredClassLoader(HealthIndicator.class))
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean("openbrokerHealthContributor"));
        runner.withPropertyValues("management.health.openbroker.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean("openbrokerHealthContributor"));
    }

    @Test
    void withoutConnectionsThereIsNoHealth() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
                .run(context -> assertThat(context.getBeansOfType(CompositeHealthContributor.class))
                        .isEmpty());
    }
}
