package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.Deadline;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SocketsTest {

    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    void anAddressIsSplitIntoItsHostAndPort() {
        assertThat(Sockets.host("broker-1:9092")).isEqualTo("broker-1");
        assertThat(Sockets.port("broker-1:9092")).isEqualTo(9092);
        assertThat(Sockets.host("[::1]:9092")).isEqualTo("::1");
    }

    @Test
    void anAddressThatAcceptsAConnectionHasNoFailure() throws IOException {
        try (ServerSocket open = new ServerSocket(0)) {
            assertThat(Sockets.open(
                            "127.0.0.1:" + open.getLocalPort(), Deadline.after(Duration.ofSeconds(5), CLOCK), CLOCK))
                    .isEmpty();
        }
    }

    @Test
    void theFailuresNameThemselvesAsTheMetadataDoes() throws IOException {
        int closed;
        try (ServerSocket probe = new ServerSocket(0)) {
            closed = probe.getLocalPort();
        }
        Deadline limit = Deadline.after(Duration.ofSeconds(5), CLOCK);

        assertThat(Sockets.open("127.0.0.1:" + closed, limit, CLOCK)).contains(Sockets.Failure.REFUSED);
        assertThat(Sockets.open("no-such-host.invalid:9092", limit, CLOCK)).contains(Sockets.Failure.DNS);
        assertThat(Sockets.Failure.DNS.text()).isEqualTo("dns");
        assertThat(Sockets.Failure.REFUSED.text()).isEqualTo("refused");
        assertThat(Sockets.Failure.TIMEOUT.text()).isEqualTo("timeout");
    }
}
