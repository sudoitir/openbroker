package io.github.sudoitir.openbroker.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class RabbitMqDriverTest {

    private static ClusterSpec spec(List<Endpoint> endpoints, Map<String, String> options) {
        return new ClusterSpec(List.of(), endpoints, options);
    }

    private static Endpoint management(String protocol) {
        return Endpoint.of(Plane.MANAGEMENT, protocol, "localhost:15672");
    }

    @Test
    void theDriverIsRegisteredThroughTheServiceLoader() {
        assertThat(ServiceLoader.load(BrokerDriver.class))
                .anyMatch(d -> d instanceof RabbitMqDriver && d.system().id().equals("rabbitmq"));
    }

    @Test
    void theDriverSupportsRabbitMq313AndLater() {
        assertThat(new RabbitMqDriver().system().supportedVersions()).isEqualTo("[3.13.0,)");
        assertThat(new RabbitMqDriver().system().filterDialects()).isEmpty();
    }

    @Test
    void theSchemaDescribesTheWatchIntervalAndTheEventVhost() {
        assertThat(new RabbitMqDriver().schema().options())
                .extracting(OptionSpec::key)
                .containsExactly("rabbitmq.watch_interval", "rabbitmq.event_vhost");
        assertThat(new RabbitMqDriver().schema().options()).noneMatch(OptionSpec::sensitive);
    }

    @Test
    void aSpecWithoutAManagementEndpointIsRefused() {
        ClusterSpec dataOnly = spec(List.of(Endpoint.of(Plane.DATA, "amqp", "localhost:5672")), Map.of());
        assertThatThrownBy(() -> RabbitMqDriver.requireShape(dataOnly))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                    assertThat(e.getMessage()).contains("management endpoint");
                });
        ClusterSpec empty = spec(List.of(), Map.of());
        assertThatThrownBy(() -> RabbitMqDriver.requireShape(empty)).isInstanceOf(BrokerException.class);
    }

    @Test
    void httpAndHttpsManagementEndpointsWithOptionalAmqpDataEndpointsAreAccepted() {
        ClusterSpec http = spec(List.of(management("http")), Map.of());
        ClusterSpec https = spec(List.of(management("https")), Map.of());
        ClusterSpec secured =
                spec(List.of(management("https"), Endpoint.of(Plane.DATA, "amqps", "localhost:5671")), Map.of());
        assertThatCode(() -> RabbitMqDriver.requireShape(http)).doesNotThrowAnyException();
        assertThatCode(() -> RabbitMqDriver.requireShape(https)).doesNotThrowAnyException();
        assertThatCode(() -> RabbitMqDriver.requireShape(secured)).doesNotThrowAnyException();
    }

    @Test
    void endpointsOnANodeCountToo() {
        ClusterSpec perNode = ClusterSpec.ofNodes(List.of(new NodeSpec(
                NodeId.of("rabbit@a"), List.of(management("http"), Endpoint.of(Plane.DATA, "amqp", "a:5672")))));
        assertThatCode(() -> RabbitMqDriver.requireShape(perNode)).doesNotThrowAnyException();
    }

    @Test
    void anEndpointWithAnotherProtocolIsRefused() {
        ClusterSpec ftp = spec(List.of(management("ftp")), Map.of());
        assertThatThrownBy(() -> RabbitMqDriver.requireShape(ftp)).isInstanceOf(BrokerException.class);
        ClusterSpec streamPlane =
                spec(List.of(management("http"), Endpoint.of(Plane.DATA, "stream", "localhost:5552")), Map.of());
        assertThatThrownBy(() -> RabbitMqDriver.requireShape(streamPlane))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void optionsDefaultToTenSecondsAndTheRootVhost() {
        ClusterSpec spec = spec(List.of(management("http")), Map.of());
        assertThat(RabbitMqOptions.watchInterval(spec)).isEqualTo(Duration.ofSeconds(10));
        assertThat(RabbitMqOptions.eventVhost(spec)).isEqualTo("/");
    }

    @Test
    void optionsAreParsedAndTheWatchIntervalNeedsAtLeastOneSecond() {
        ClusterSpec set = spec(
                List.of(management("http")),
                Map.of("rabbitmq.watch_interval", "PT2S", "rabbitmq.event_vhost", "events"));
        assertThat(RabbitMqOptions.watchInterval(set)).isEqualTo(Duration.ofSeconds(2));
        assertThat(RabbitMqOptions.eventVhost(set)).isEqualTo("events");
        for (String bad : List.of("PT0.5S", "ten seconds")) {
            ClusterSpec spec = spec(List.of(management("http")), Map.of("rabbitmq.watch_interval", bad));
            assertThatThrownBy(() -> RabbitMqOptions.watchInterval(spec))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
            assertThatThrownBy(() -> RabbitMqDriver.requireShape(spec)).isInstanceOf(BrokerException.class);
        }
    }

    @Test
    void connectingChecksTheShapeBeforeAnythingElse() {
        ClusterSpec none = spec(List.of(), Map.of());
        RabbitMqDriver driver = new RabbitMqDriver();
        assertThatThrownBy(() -> driver.connect(none, null, null))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }
}
