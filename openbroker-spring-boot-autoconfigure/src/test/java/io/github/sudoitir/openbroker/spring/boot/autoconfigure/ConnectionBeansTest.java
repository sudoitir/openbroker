package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.Transport;
import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ConnectionBeansTest {

    private final FakeDriver driver = new FakeDriver();
    private final BrokerDrivers drivers = BrokerDrivers.of(List.of(driver));
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
            .withBean(BrokerDrivers.class, () -> drivers);

    @Test
    void aConfiguredConnectionIsAClusterConnectionAndATransportBean() {
        runner.withPropertyValues(
                        "openbroker.connections.orders.system=fake",
                        "openbroker.connections.orders.cluster.cluster-endpoints[0].plane=data",
                        "openbroker.connections.orders.cluster.cluster-endpoints[0].protocol=fake",
                        "openbroker.connections.orders.cluster.cluster-endpoints[0].address=a:1",
                        "openbroker.connections.orders.cluster.options.mode=b")
                .run(context -> {
                    assertThat(context).hasBean("ordersClusterConnection").hasBean("ordersTransport");
                    assertThat(context.getBean(ClusterConnection.class))
                            .isSameAs(context.getBean("ordersClusterConnection"));
                    assertThat(context.getBean(Transport.class)).isSameAs(context.getBean("ordersTransport"));
                    assertThat(driver.requests).hasSize(1);
                    assertThat(driver.requests.getFirst().cluster().clusterEndpoints())
                            .containsExactly(new Endpoint(Plane.DATA, "fake", "a:1"));
                    assertThat(driver.requests.getFirst().cluster().options()).containsEntry("mode", "b");
                });
    }

    @Test
    void theConnectionsOtherDefaultsAreReadable() {
        runner.withPropertyValues(
                        "openbroker.connections.orders.system=fake",
                        "openbroker.connections.orders.sender.guarantee=transacted")
                .run(context -> assertThat(context.getBean(OpenBrokerConnections.class)
                                .connection("orders")
                                .sender()
                                .guarantee())
                        .isEqualTo(Guarantee.TRANSACTED));
    }

    @Test
    void twoConnectionsAreInjectedByQualifierAndNotByType() {
        runner.withPropertyValues(
                        "openbroker.connections.orders.system=fake", "openbroker.connections.billing.system=fake")
                .run(context -> {
                    Transport orders = BeanFactoryAnnotationUtils.qualifiedBeanOfType(
                            context.getBeanFactory(), Transport.class, "orders");
                    Transport billing = BeanFactoryAnnotationUtils.qualifiedBeanOfType(
                            context.getBeanFactory(), Transport.class, "billing");

                    assertThat(orders)
                            .isSameAs(context.getBean("ordersTransport"))
                            .isNotSameAs(billing);
                    assertThat(billing).isSameAs(context.getBean("billingTransport"));
                    assertThatThrownBy(() -> context.getBean(Transport.class))
                            .isInstanceOf(NoUniqueBeanDefinitionException.class)
                            .hasMessageContaining("billingTransport")
                            .hasMessageContaining("ordersTransport");
                });
    }

    @Test
    void anApplicationBeanOfTheSameNameWinsAndTheStarterBacksOff() {
        Transport own = (Transport) Proxy.newProxyInstance(
                Transport.class.getClassLoader(), new Class<?>[] {Transport.class}, (p, m, a) -> null);
        runner.withBean("ordersTransport", Transport.class, () -> own)
                .withPropertyValues("openbroker.connections.orders.system=fake")
                .run(context -> {
                    assertThat(context.getBean("ordersTransport")).isSameAs(own);
                    assertThat(context.getBeansOfType(Transport.class)).hasSize(1);
                    assertThat(context).hasBean("ordersClusterConnection");
                });
    }

    @Test
    void aDriverWithoutMessagingGivesNoTransport() {
        driver.messaging = false;
        runner.withPropertyValues("openbroker.connections.orders.system=fake").run(context -> {
            assertThat(context).hasBean("ordersClusterConnection");
            assertThat(context.getBeanProvider(Transport.class).getIfAvailable())
                    .isNull();
        });
    }

    @Test
    void propertiesGiveEveryPlaneTheirCredentialsUnlessManagementHasItsOwn() {
        runner.withPropertyValues(
                        "openbroker.connections.orders.system=fake",
                        "openbroker.connections.orders.credentials.form=username-password",
                        "openbroker.connections.orders.credentials.username=app",
                        "openbroker.connections.orders.credentials.password=" + TestSecrets.PASSWORD,
                        "openbroker.connections.orders.management-credentials.form=none")
                .run(context -> {
                    CredentialSource source = driver.requests.getFirst().credentials();

                    assertThat(source.forPlane(Plane.DATA))
                            .isEqualTo(Credentials.usernamePassword("app", TestSecrets.PASSWORD));
                    assertThat(source.forPlane(Plane.MANAGEMENT)).isEqualTo(new Credentials.None());
                });
    }

    @Test
    void aCredentialSourceBeanQualifiedByTheNameWinsOverProperties() {
        CredentialSource own = CredentialSource.of(Credentials.usernamePassword("bean", TestSecrets.OTHER_PASSWORD));
        runner.withBean("orders", CredentialSource.class, () -> own)
                .withPropertyValues(
                        "openbroker.connections.orders.system=fake",
                        "openbroker.connections.orders.credentials.form=username-password",
                        "openbroker.connections.orders.credentials.username=app",
                        "openbroker.connections.orders.credentials.password=" + TestSecrets.PASSWORD)
                .run(context ->
                        assertThat(driver.requests.getFirst().credentials()).isSameAs(own));
    }

    @Test
    void connectionsCloseBeforeTheDriversDo() {
        driver.driversOpen = () -> {
            try {
                drivers.descriptor(FakeDriver.SYSTEM);
                return true;
            } catch (IllegalStateException _) {
                return false;
            }
        };
        runner.withPropertyValues("openbroker.connections.orders.system=fake").run(context -> {});

        assertThat(driver.eventsCopy()).containsExactly("closed 1 with drivers open");
        assertThatThrownBy(() -> drivers.descriptor(FakeDriver.SYSTEM)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aMisspeltOptionFailsTheStartNamingTheConnectionAndTheKey() {
        runner.withPropertyValues(
                        "openbroker.connections.orders.system=fake",
                        "openbroker.connections.orders.cluster.options.moode=a")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(ConnectionConfigurationException.class);
                    assertThat(new ConnectionFailureAnalyzer().analyze(context.getStartupFailure()))
                            .satisfies(analysis -> assertThat(analysis.getDescription())
                                    .contains("orders")
                                    .contains("openbroker.connections.orders.cluster.options.moode"));
                    assertThat(driver.requests).isEmpty();
                });
    }

    @Test
    void aMissingDriverFailsTheStart() {
        runner.withPropertyValues("openbroker.connections.orders.system=pulsar").run(context -> {
            assertThat(context).hasFailed();
            assertThat(new ConnectionFailureAnalyzer()
                            .analyze(context.getStartupFailure())
                            .getDescription())
                    .contains("pulsar");
        });
    }

    @Test
    void noConfigurationGivesNoConnections() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(ClusterConnection.class).doesNotHaveBean(Transport.class);
            assertThat(context.getBean(OpenBrokerConnections.class).names()).isEmpty();
        });
    }
}
