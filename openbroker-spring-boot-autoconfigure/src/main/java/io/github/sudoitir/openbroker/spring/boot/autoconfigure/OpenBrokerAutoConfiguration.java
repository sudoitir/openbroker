package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.opentelemetry.OpenTelemetryMessaging;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.artemis.autoconfigure.ArtemisConnectionDetails;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * OpenBroker for a Spring Boot application: the drivers, the pieces a connection is assembled from
 * (connect options, TLS from SSL bundles), one connection per configured or derived system, and their
 * health, metrics, tracing and lifecycle. Every bean backs off to an application bean of its type.
 */
@AutoConfiguration(
        after = SslAutoConfiguration.class,
        afterName = {
            "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
            "org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration",
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
            "org.springframework.boot.artemis.autoconfigure.ArtemisAutoConfiguration"
        })
@EnableConfigurationProperties(OpenBrokerProperties.class)
public class OpenBrokerAutoConfiguration {

    /** The drivers on the class path. Beans that use them are destroyed first, so it closes last. */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    BrokerDrivers brokerDrivers() {
        return BrokerDrivers.load();
    }

    @Bean
    @ConditionalOnMissingBean
    OpenBrokerConnectOptions openBrokerConnectOptions(
            OpenBrokerProperties properties,
            ObjectProvider<CallInterceptor> interceptor,
            ObjectProvider<Clock> clock,
            ObjectProvider<OpenBrokerConnectOptions.ContextPropagation> contextPropagation,
            ObjectProvider<ObserverSource> observers,
            ObjectProvider<ConnectOptionsCustomizer> customizers) {
        return new OpenBrokerConnectOptions(properties, interceptor, clock, contextPropagation, observers, customizers);
    }

    @Bean
    static OpenBrokerConnectionRegistrar openBrokerConnectionRegistrar(Environment environment) {
        return new OpenBrokerConnectionRegistrar(environment);
    }

    /** The connections' binding and opening; the beans the registrar defines are made by it. */
    @Bean(OpenBrokerConnections.BEAN_NAME)
    @ConditionalOnMissingBean
    OpenBrokerConnections openBrokerConnections(
            Environment environment,
            BrokerDrivers drivers,
            OpenBrokerConnectOptions connectOptions,
            ObjectProvider<SslBundleTls> ssl,
            ObjectProvider<ConnectionDeriver> derivers,
            ObjectProvider<ObserverSource> observers,
            ListableBeanFactory beans) {
        return new OpenBrokerConnections(environment, drivers, connectOptions, ssl, derivers, observers, beans);
    }

    /** The processors the application registers: started when the context is ready, stopped before connections close. */
    @Bean
    @ConditionalOnMissingBean
    OpenBrokerProcessors openBrokerProcessors(BeanFactory beans, OpenBrokerConnections connections) {
        return new OpenBrokerProcessors(beans, connections);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(SslBundles.class)
    static class SslConfiguration {

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnBean(SslBundles.class)
        SslBundleTls sslBundleTls(SslBundles bundles) {
            return new SslBundleTls(bundles);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ContextSnapshotFactory.class)
    static class ContextPropagationConfiguration {

        @Bean
        @ConditionalOnMissingBean
        OpenBrokerConnectOptions.ContextPropagation openBrokerContextPropagation(
                ObjectProvider<ContextSnapshotFactory> factory) {
            ContextSnapshotFactory snapshots = factory.getIfAvailable(
                    () -> ContextSnapshotFactory.builder().build());
            return new OpenBrokerConnectOptions.ContextPropagation(
                    runnable -> snapshots.captureAll().wrap(runnable));
        }
    }

    /** The {@code kafka} connection from Spring Boot's Kafka connection details, with the Kafka driver. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(
            name = {
                "org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails",
                "io.github.sudoitir.openbroker.kafka.KafkaDriver"
            })
    @ConditionalOnProperty(prefix = "openbroker.spring-connections", name = "enabled", matchIfMissing = true)
    static class KafkaDerivation {

        @Bean
        @ConditionalOnMissingBean(KafkaConnectionDeriver.class)
        KafkaConnectionDeriver kafkaConnectionDeriver(
                ObjectProvider<KafkaConnectionDetails> details,
                Environment environment,
                ObjectProvider<SslBundleTls> ssl) {
            return new KafkaConnectionDeriver(details, environment, ssl);
        }
    }

    /** The {@code artemis} connection from Spring Boot's Artemis connection details, with the Artemis driver. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(
            name = {
                "org.springframework.boot.artemis.autoconfigure.ArtemisConnectionDetails",
                "io.github.sudoitir.openbroker.artemis.ArtemisDriver"
            })
    @ConditionalOnProperty(prefix = "openbroker.spring-connections", name = "enabled", matchIfMissing = true)
    static class ArtemisDerivation {

        @Bean
        @ConditionalOnMissingBean(ArtemisConnectionDeriver.class)
        ArtemisConnectionDeriver artemisConnectionDeriver(
                ObjectProvider<ArtemisConnectionDetails> details, Environment environment) {
            return new ArtemisConnectionDeriver(details, environment);
        }
    }

    /** Health under {@code openbroker}: one indicator for each connection. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnEnabledHealthIndicator("openbroker")
    static class HealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "openbrokerHealthContributor")
        @Nullable OpenBrokerHealth openbrokerHealthContributor(
                OpenBrokerConnections connections, ListableBeanFactory beans, OpenBrokerProperties properties) {
            Map<String, ConnectionHealthIndicator> indicators = new LinkedHashMap<>();
            for (String name : connections.names()) {
                String beanName = name + OpenBrokerConnectionRegistrar.CLUSTER_CONNECTION;
                ClusterConnection connection =
                        beans.containsBean(beanName) && beans.getBean(beanName) instanceof ClusterConnection opened
                                ? opened
                                : null;
                if (connection != null) {
                    indicators.put(
                            name,
                            new ConnectionHealthIndicator(
                                    connection, properties.health().timeout()));
                }
            }
            if (indicators.isEmpty()) {
                return null;
            }
            return new OpenBrokerHealth(CompositeHealthContributor.fromMap(indicators), indicators.values());
        }
    }

    /** Combines what Micrometer and OpenTelemetry contribute; nothing is wired when neither is present. */
    @Bean
    @ConditionalOnMissingBean(ObserverSource.class)
    @ConditionalOnBean(ObserverContribution.class)
    OpenBrokerObservers openBrokerObservers(ObjectProvider<ObserverContribution> contributions) {
        return new OpenBrokerObservers(contributions.orderedStream().toList());
    }

    /** Metrics go to the application's Micrometer registry. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class MicrometerObservation {

        @Bean
        @ConditionalOnBean(MeterRegistry.class)
        @ConditionalOnMissingBean(MicrometerContribution.class)
        MicrometerContribution micrometerObserverContribution(MeterRegistry registry) {
            return new MicrometerContribution(registry);
        }
    }

    /** Spans and metrics go to the application's OpenTelemetry when Micrometer is not recording the metrics. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({OpenTelemetry.class, OpenTelemetryMessaging.class})
    @ConditionalOnBean(OpenTelemetry.class)
    @ConditionalOnMissingBean(type = "io.micrometer.core.instrument.MeterRegistry")
    static class OpenTelemetryObservation {

        @Bean
        @ConditionalOnMissingBean(OpenTelemetryContribution.class)
        OpenTelemetryContribution openTelemetryObserverContribution(
                OpenTelemetry openTelemetry, OpenBrokerProperties properties) {
            return new OpenTelemetryContribution(
                    openTelemetry, true, properties.tracing().injectContext());
        }
    }

    /** Only spans go to OpenTelemetry when Micrometer records the metrics, so nothing is counted twice. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({OpenTelemetry.class, OpenTelemetryMessaging.class})
    @ConditionalOnBean(value = OpenTelemetry.class, type = "io.micrometer.core.instrument.MeterRegistry")
    static class OpenTelemetrySpansOnlyObservation {

        @Bean
        @ConditionalOnMissingBean(OpenTelemetryContribution.class)
        OpenTelemetryContribution spansOnlyObserverContribution(
                OpenTelemetry openTelemetry, OpenBrokerProperties properties) {
            return new OpenTelemetryContribution(
                    openTelemetry, false, properties.tracing().injectContext());
        }
    }
}
