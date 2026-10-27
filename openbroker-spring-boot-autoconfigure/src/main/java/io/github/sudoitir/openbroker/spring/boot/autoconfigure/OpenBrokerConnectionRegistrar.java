package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.Transport;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * Defines two beans for each connection, {@code <name>ClusterConnection} and {@code <name>Transport},
 * each qualified by the connection's name. With exactly one connection its beans are primary. A bean of
 * the same name that the application defined is kept.
 */
final class OpenBrokerConnectionRegistrar implements BeanDefinitionRegistryPostProcessor {

    private static final List<DerivedSystem> DERIVED_SYSTEMS = List.of(
            new DerivedSystem(
                    KafkaConnectionDeriver.NAME,
                    "org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails",
                    "io.github.sudoitir.openbroker.kafka.KafkaDriver"),
            new DerivedSystem(
                    ArtemisConnectionDeriver.NAME,
                    "org.springframework.boot.artemis.autoconfigure.ArtemisConnectionDetails",
                    "io.github.sudoitir.openbroker.artemis.ArtemisDriver"));

    static final String CLUSTER_CONNECTION = "ClusterConnection";
    static final String TRANSPORT = "Transport";

    private final Environment environment;

    OpenBrokerConnectionRegistrar(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        Set<String> names = new TreeSet<>(OpenBrokerConnections.configuredNames(environment));
        names.addAll(derivable((ConfigurableListableBeanFactory) registry));
        boolean primary = names.size() == 1;
        for (String name : names) {
            register(registry, name + CLUSTER_CONNECTION, definition(ClusterConnection.class, "open", name, primary));
            RootBeanDefinition transport = definition(Transport.class, "transport", name, primary);
            transport
                    .getConstructorArgumentValues()
                    .addIndexedArgumentValue(1, new RuntimeBeanReference(name + CLUSTER_CONNECTION));
            register(registry, name + TRANSPORT, transport);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        // Everything is defined in postProcessBeanDefinitionRegistry.
    }

    /**
     * The systems whose connection Spring's details can describe: their driver and Boot's details type
     * are present, Boot defines a details bean, derivation is on, and the broker is not embedded.
     */
    private Set<String> derivable(ConfigurableListableBeanFactory beanFactory) {
        Set<String> names = new TreeSet<>();
        if (!environment.getProperty("openbroker.spring-connections.enabled", Boolean.class, true)) {
            return names;
        }
        ClassLoader loader = beanFactory.getBeanClassLoader();
        for (DerivedSystem system : DERIVED_SYSTEMS) {
            if (ClassUtils.isPresent(system.driver, loader)
                    && ClassUtils.isPresent(system.details, loader)
                    && hasBean(beanFactory, system.details, loader)
                    && !(system.name.equals(ArtemisConnectionDeriver.NAME) && embeddedByConfiguration())) {
                names.add(system.name);
            }
        }
        return names;
    }

    private static boolean hasBean(
            ConfigurableListableBeanFactory beanFactory, String detailsType, @Nullable ClassLoader loader) {
        try {
            return beanFactory.getBeanNamesForType(ClassUtils.forName(detailsType, loader), true, false).length > 0;
        } catch (ClassNotFoundException _) {
            return false;
        }
    }

    /** Embedded by what the properties say, which is all that is known before any bean exists. */
    private boolean embeddedByConfiguration() {
        String mode = environment.getProperty("spring.artemis.mode");
        return mode == null
                ? ArtemisConnectionDeriver.embeddedByDefault(environment)
                : mode.equalsIgnoreCase("embedded");
    }

    private record DerivedSystem(String name, String details, String driver) {}

    private static RootBeanDefinition definition(Class<?> type, String factoryMethod, String name, boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(type);
        definition.setTargetType(type);
        definition.setFactoryBeanName(OpenBrokerConnections.BEAN_NAME);
        definition.setUniqueFactoryMethodName(factoryMethod);
        definition.getConstructorArgumentValues().addIndexedArgumentValue(0, name);
        definition.setPrimary(primary);
        definition.addQualifier(new AutowireCandidateQualifier(Qualifier.class, name));
        if (type == ClusterConnection.class) {
            definition.setDestroyMethodName(AbstractBeanDefinition.INFER_METHOD);
        }
        return definition;
    }

    private static void register(BeanDefinitionRegistry registry, String beanName, RootBeanDefinition definition) {
        if (!registry.containsBeanDefinition(beanName)) {
            registry.registerBeanDefinition(beanName, definition);
        }
    }
}
