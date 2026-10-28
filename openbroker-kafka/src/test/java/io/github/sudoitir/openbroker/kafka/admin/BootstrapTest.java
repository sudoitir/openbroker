package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BootstrapTest {

    private static final Clock CLOCK = Clock.systemUTC();

    private static int closedPort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    void aNameThatDoesNotResolveFailsAtOnceAndSaysSo() {
        assertThatThrownBy(() -> Bootstrap.reachable(List.of("kafka-x.invalid:9092"), Duration.ofSeconds(5), CLOCK))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.UNREACHABLE);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                    assertThat(e.getMessage()).isEqualTo("kafka-x.invalid:9092 does not resolve.");
                    assertThat(e.info().metadata())
                            .containsEntry("kafka.endpoint", "kafka-x.invalid:9092")
                            .containsEntry("kafka.reach_failure", "dns");
                });
    }

    @Test
    void aClosedPortFailsAsRefusedWithoutWaitingForTheDeadline() throws IOException {
        String server = "127.0.0.1:" + closedPort();
        long started = System.nanoTime();

        assertThatThrownBy(() -> Bootstrap.reachable(List.of(server), Duration.ofSeconds(10), CLOCK))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.info().metadata()).containsEntry("kafka.reach_failure", "refused");
                    assertThat(e.getMessage()).isEqualTo(server + " refuses connections.");
                });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
    }

    @Test
    void theServersThatAnswerAreKeptAndTheOthersDropped() throws IOException {
        try (ServerSocket open = new ServerSocket(0)) {
            String up = "127.0.0.1:" + open.getLocalPort();

            assertThat(Bootstrap.reachable(
                            List.of("kafka-x.invalid:9092", up, "127.0.0.1:" + closedPort()),
                            Duration.ofSeconds(5),
                            CLOCK))
                    .containsExactly(up);
        }
    }

    @Test
    void whenNoneAnswersTheFirstCausesLeadsAndEveryServerIsListed() throws IOException {
        String closed = "127.0.0.1:" + closedPort();

        assertThatThrownBy(() ->
                        Bootstrap.reachable(List.of("kafka-x.invalid:9092", closed), Duration.ofSeconds(5), CLOCK))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.info().metadata()).containsEntry("kafka.reach_failure", "dns");
                    assertThat(e.getMessage())
                            .isEqualTo("kafka-x.invalid:9092 does not resolve; " + closed + " refuses connections.");
                });
    }
}
