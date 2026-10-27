package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.micrometer.context.ContextSnapshotFactory;
import java.time.Clock;
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
}
