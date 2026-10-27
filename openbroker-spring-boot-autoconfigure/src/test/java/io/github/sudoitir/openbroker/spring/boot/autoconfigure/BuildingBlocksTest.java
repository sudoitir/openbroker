package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import io.micrometer.context.ContextSnapshotFactory;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundleRegistry;
import org.springframework.boot.ssl.SslStoreBundle;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.Ordered;

class BuildingBlocksTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class));

    /** A customizer that runs at a given place in the order and records that it ran. */
    private record Customizer(int order, List<String> ran, String name) implements ConnectOptionsCustomizer, Ordered {

        @Override
        public ConnectOptions customize(String connection, String system, ConnectOptions options) {
            ran.add(name + ":" + connection + ":" + system);
            return options;
        }

        @Override
        public int getOrder() {
            return order;
        }
    }

    @Test
    void connectOptionsCarryTheApplicationsBeansAndTheClientDefaults() {
        CallInterceptor interceptor = call -> io.github.sudoitir.openbroker.Permit.unlimited();
        Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
        runner.withBean(CallInterceptor.class, () -> interceptor)
                .withBean(Clock.class, () -> clock)
                .withPropertyValues(
                        "openbroker.client.connect-timeout=7s",
                        "openbroker.client.call-timeout=30s",
                        "openbroker.client.max-response-bytes=2048",
                        "openbroker.client.spool-directory=/var/spool/ob")
                .run(context -> {
                    ConnectOptions options = context.getBean(OpenBrokerConnectOptions.class)
                            .forConnection("c1", "artemis", TlsProvider.none());

                    assertThat(options.interceptor()).isSameAs(interceptor);
                    assertThat(options.clock()).isSameAs(clock);
                    assertThat(options.timeouts().connect()).isEqualTo(Duration.ofSeconds(7));
                    assertThat(options.timeouts().call()).isEqualTo(Duration.ofSeconds(30));
                    assertThat(options.maxResponseBytes()).isEqualTo(2048);
                    assertThat(options.spoolDirectory()).isEqualTo(Path.of("/var/spool/ob"));
                });
    }

    @Test
    void theSdksDefaultsFillWhatIsNotSet() {
        runner.run(context -> {
            ConnectOptions options =
                    context.getBean(OpenBrokerConnectOptions.class).forConnection("c1", "kafka", TlsProvider.none());

            assertThat(options.timeouts().connect())
                    .isEqualTo(ConnectOptions.defaults().timeouts().connect());
            assertThat(options.maxResponseBytes())
                    .isEqualTo(ConnectOptions.defaults().maxResponseBytes());
            assertThat(options.spoolDirectory())
                    .isEqualTo(ConnectOptions.defaults().spoolDirectory());
        });
    }

    @Test
    void aConnectionsOwnKeysWinOverTheClientDefaults() {
        runner.withPropertyValues("openbroker.client.connect-timeout=7s").run(context -> {
            ConnectionOptions own = new ConnectionOptions(Duration.ofSeconds(1), null, null, null, null, null);

            ConnectOptions options = context.getBean(OpenBrokerConnectOptions.class)
                    .forConnection("c1", "kafka", TlsProvider.none(), own);

            assertThat(options.timeouts().connect()).isEqualTo(Duration.ofSeconds(1));
        });
    }

    @Test
    void customizersRunLastInOrder() {
        List<String> ran = new ArrayList<>();
        runner.withBean("late", Customizer.class, () -> new Customizer(2, ran, "late"))
                .withBean("early", Customizer.class, () -> new Customizer(1, ran, "early"))
                .run(context -> {
                    context.getBean(OpenBrokerConnectOptions.class).forConnection("c1", "artemis", TlsProvider.none());

                    assertThat(ran).containsExactly("early:c1:artemis", "late:c1:artemis");
                });
    }

    @Test
    void aCustomizerSeesWhatTheOtherBeansSet() {
        Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
        List<Clock> seen = new ArrayList<>();
        runner.withBean(Clock.class, () -> clock)
                .withBean(ConnectOptionsCustomizer.class, () -> (name, system, options) -> {
                    seen.add(options.clock());
                    return options.withMaxResponseBytes(99);
                })
                .run(context -> {
                    ConnectOptions options = context.getBean(OpenBrokerConnectOptions.class)
                            .forConnection("c1", "artemis", TlsProvider.none());

                    assertThat(seen).containsExactly(clock);
                    assertThat(options.maxResponseBytes()).isEqualTo(99);
                });
    }

    @Test
    void contextPropagationComesFromMicrometerWhenPresent() {
        Runnable task = () -> {};
        runner.run(context -> assertThat(context.getBean(OpenBrokerConnectOptions.class)
                        .forConnection("c1", "kafka", TlsProvider.none())
                        .contextPropagation()
                        .apply(task))
                .isNotSameAs(task));
        runner.withClassLoader(new FilteredClassLoader(ContextSnapshotFactory.class))
                .run(context -> assertThat(context.getBean(OpenBrokerConnectOptions.class)
                                .forConnection("c1", "kafka", TlsProvider.none())
                                .contextPropagation()
                                .apply(task))
                        .isSameAs(task));
    }

    @Test
    void theDriversCloseWithTheContext() {
        BrokerDrivers[] drivers = new BrokerDrivers[1];
        runner.run(context -> drivers[0] = context.getBean(BrokerDrivers.class));

        assertThatThrownBy(() -> drivers[0].descriptor("fake")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anApplicationsDriversWin() {
        BrokerDrivers own = BrokerDrivers.of(List.of(new FakeDriver()));
        runner.withBean(BrokerDrivers.class, () -> own)
                .run(context -> assertThat(context.getBean(BrokerDrivers.class)).isSameAs(own));
    }

    @Test
    void aMissingSslBundleIsUnavailableAndNamesTheBundle() {
        runner.run(context -> {
            SslBundleTls tls = context.getBean(SslBundleTls.class);

            assertThatThrownBy(() -> tls.forBundle("nope", true)).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
                assertThat(e.reason()).isEqualTo(ErrorReasons.TLS_HANDSHAKE_FAILED);
                assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                assertThat(e.getMessage()).contains("'nope'").contains("spring.ssl.bundle");
            });
        });
    }

    @Test
    void aReloadedBundleReachesTheNextConnectAndIsAnnounced() {
        runner.run(context -> {
            SslBundleRegistry registry = context.getBean(SslBundleRegistry.class);
            registry.registerBundle("kafka", SslBundle.of(SslStoreBundle.NONE));
            TlsProvider provider = context.getBean(SslBundleTls.class).forBundle("kafka", false);
            AtomicInteger changes = new AtomicInteger();
            provider.onChange(changes::incrementAndGet);

            TlsMaterial before = provider.current().orElseThrow();
            assertThat(provider.current().orElseThrow()).isSameAs(before);
            assertThat(before.hostnameVerification()).isFalse();

            registry.updateBundle("kafka", SslBundle.of(SslStoreBundle.NONE));

            assertThat(changes).hasValue(1);
            TlsMaterial after = provider.current().orElseThrow();
            assertThat(after.id()).isNotEqualTo(before.id());
            assertThat(after.context()).isNotSameAs(before.context());
        });
    }

    @Test
    void aBundleObjectGivesFixedMaterial() {
        runner.run(context -> {
            TlsProvider provider =
                    context.getBean(SslBundleTls.class).forBundle(SslBundle.of(SslStoreBundle.NONE), true);

            assertThat(provider.current()).isPresent();
            assertThat(provider.current().orElseThrow().hostnameVerification()).isTrue();
        });
    }

    @Test
    void theSameBundleIsWatchedOnce() {
        runner.run(context -> {
            DefaultSslBundleRegistry registry = (DefaultSslBundleRegistry) context.getBean(SslBundleRegistry.class);
            registry.registerBundle("kafka", SslBundle.of(SslStoreBundle.NONE));
            SslBundleTls tls = context.getBean(SslBundleTls.class);

            assertThat(tls.forBundle("kafka", true)).isSameAs(tls.forBundle("kafka", true));
        });
    }
}
