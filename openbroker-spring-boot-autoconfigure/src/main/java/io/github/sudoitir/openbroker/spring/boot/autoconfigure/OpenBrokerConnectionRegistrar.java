package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.Transport;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.env.Environment;

/**
 * Defines two beans for each connection, {@code <name>ClusterConnection} and {@code <name>Transport},
 * each qualified by the connection's name. With exactly one connection its beans are primary. A bean of
 * the same name that the application defined is kept.
 */
final class OpenBrokerConnectionRegistrar implements BeanDefinitionRegistryPostProcessor {

    static final String CLUSTER_CONNECTION = "ClusterConnection";
    static final String TRANSPORT = "Transport";

    private final Environment environment;

    OpenBrokerConnectionRegistrar(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        Set<String> names = new TreeSet<>(OpenBrokerConnections.configuredNames(environment));
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
