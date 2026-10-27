package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class ProcessorLifecycleTest {

    private static final ResourceId SUBSCRIPTION = ResourceId.of("fake.queue", "orders");
    private static final MessageHandler HANDLER = message -> new Disposition.Accept();

    private final FakeDriver driver = new FakeDriver();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
            .withBean(BrokerDrivers.class, () -> BrokerDrivers.of(List.of(driver)))
            .withPropertyValues("openbroker.connections.orders.system=fake");

    /** Registers a processor while the context is still starting, as an application bean would. */
    static class EagerRegistrant {

        private final OpenBrokerProcessors processors;

        EagerRegistrant(OpenBrokerProcessors processors) {
            this.processors = processors;
        }

        @PostConstruct
        void register() {
            processors.register("orders", SUBSCRIPTION, Scope.whole(), HANDLER);
        }
    }

    /** What Spring Boot publishes once the runners have run; the context runner does not. */
    private static void ready(ConfigurableApplicationContext context) {
        context.publishEvent(new ApplicationReadyEvent(new SpringApplication(), new String[0], context, Duration.ZERO));
    }

    @Test
    void aProcessorStartsOnlyWhenTheApplicationIsReady() {
        List<Integer> startedWhenSingletonsWereReady = new CopyOnWriteArrayList<>();
        runner.withBean(EagerRegistrant.class)
                .withBean(
                        SmartInitializingSingleton.class,
                        () -> () -> startedWhenSingletonsWereReady.add((int) driver.events.stream()
                                .filter("process started"::equals)
                                .count()))
                .run(context -> {
                    assertThat(startedWhenSingletonsWereReady).containsExactly(0);
                    assertThat(driver.eventsCopy()).doesNotContain("process started");

                    ready(context);

                    assertThat(driver.eventsCopy()).contains("process started");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class})
    static class RunnerApplication {

        static final List<String> ORDER = new CopyOnWriteArrayList<>();
        static FakeDriver driver;

        @Bean
        BrokerDrivers brokerDrivers() {
            return BrokerDrivers.of(List.of(driver));
        }

        @Bean
        EagerRegistrant eagerRegistrant(OpenBrokerProcessors processors) {
            return new EagerRegistrant(processors);
        }

        @Bean
        ApplicationRunner runner() {
            return (ApplicationArguments args) -> {
                ORDER.add("runner saw " + driver.eventsCopy().size() + " events");
            };
        }
    }

    @Test
    void noHandlerStartsBeforeTheApplicationRunnersHaveRun() {
        RunnerApplication.driver = driver;
        RunnerApplication.ORDER.clear();
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(RunnerApplication.class)
                .web(WebApplicationType.NONE)
                .properties("openbroker.connections.orders.system=fake")
                .run()) {
            assertThat(RunnerApplication.ORDER).containsExactly("runner saw 0 events");
            assertThat(driver.eventsCopy()).contains("process started");
        }
    }

    @Test
    void aProcessorRegisteredLaterStartsAtOnce() {
        runner.run(context -> {
            ready(context);
            assertThat(driver.eventsCopy()).doesNotContain("process started");

            context.getBean(OpenBrokerProcessors.class).register("orders", SUBSCRIPTION, Scope.whole(), HANDLER);

            assertThat(driver.eventsCopy()).contains("process started");
        });
    }

    @Test
    void shutdownStopsProcessorsWithinTheirGraceBeforeTheConnectionsClose() {
        runner.withBean(EagerRegistrant.class).run(ProcessorLifecycleTest::ready);

        assertThat(driver.eventsCopy())
                .containsExactly(
                        "process started",
                        "processor stopped within "
                                + ProcessorOptions.defaults().stopGrace(),
                        "closed 1 with drivers open");
    }

    @Test
    void aProcessorStopsWithinTheGraceOfTheOptionsItWasGiven() {
        ProcessorOptions defaults = ProcessorOptions.defaults();
        ProcessorOptions options = new ProcessorOptions(
                defaults.receive(), 1, defaults.ordering(), defaults.retry(), Duration.ofSeconds(5));
        runner.run(context -> {
            ready(context);
            context.getBean(OpenBrokerProcessors.class)
                    .register(context.getBean(Transport.class), SUBSCRIPTION, Scope.whole(), options, HANDLER);
        });

        assertThat(driver.eventsCopy().stream().filter(e -> e.startsWith("processor stopped")))
                .containsExactly("processor stopped within PT5S");
    }

    @Test
    void aStoppedAndStartedContextStartsTheProcessorsAgain() {
        runner.withBean(EagerRegistrant.class).run(context -> {
            ready(context);
            context.stop();
            context.start();

            assertThat(driver.eventsCopy())
                    .containsExactly(
                            "process started",
                            "processor stopped within "
                                    + ProcessorOptions.defaults().stopGrace(),
                            "process started");
        });
    }

    @Test
    void aConnectionWithoutMessagingCannotHaveAProcessor() {
        driver.messaging = false;
        runner.run(context -> assertThatThrownBy(() -> context.getBean(OpenBrokerProcessors.class)
                        .register("orders", SUBSCRIPTION, Scope.whole(), HANDLER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Connection 'orders' has no messaging transport"));
    }

    @Test
    void nothingCanBeRegisteredOnceShutdownBegins() {
        OpenBrokerProcessors[] processors = new OpenBrokerProcessors[1];
        Transport[] transport = new Transport[1];
        runner.run(context -> {
            processors[0] = context.getBean(OpenBrokerProcessors.class);
            transport[0] = context.getBean(Transport.class);
        });

        assertThatThrownBy(() -> processors[0].register(
                        transport[0], SUBSCRIPTION, Scope.whole(), ProcessorOptions.defaults(), HANDLER))
                .isInstanceOf(IllegalStateException.class);
    }
}
