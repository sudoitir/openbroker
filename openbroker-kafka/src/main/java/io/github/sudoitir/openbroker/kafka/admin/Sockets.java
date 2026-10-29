package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.Optional;

/** Whether a {@code host:port} address resolves and accepts a connection, before any Kafka client is made for it. */
final class Sockets {

    /** Why an address cannot be reached. */
    enum Failure {
        DNS("dns"),
        REFUSED("refused"),
        TIMEOUT("timeout");

        private final String text;

        Failure(String text) {
            this.text = text;
        }

        /** The value of the error metadata {@code kafka.reach_failure}. */
        String text() {
            return text;
        }
    }

    private Sockets() {}

    /** The host of an address, without the brackets an IPv6 literal is written in. */
    static String host(String address) {
        String host = address.substring(0, address.lastIndexOf(':'));
        return host.length() > 1 && host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1)
                : host;
    }

    static int port(String address) {
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }

    /** Resolves the address and opens, then closes, a connection to it within {@code limit}; empty when it works. */
    static Optional<Failure> open(String address, Deadline limit, Clock clock) {
        InetAddress[] resolved;
        try {
            resolved = InetAddress.getAllByName(host(address));
        } catch (UnknownHostException _) {
            return Optional.of(Failure.DNS);
        }
        boolean timedOut = false;
        for (InetAddress candidate : resolved) {
            int millis = Math.clamp(limit.remaining(clock).toMillis(), 1, Integer.MAX_VALUE);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(candidate, port(address)), millis);
                return Optional.empty();
            } catch (ConnectException _) {
                // Refused; the next address may answer.
            } catch (IOException _) {
                // A timeout or any other failure to open the connection.
                timedOut = true;
            }
        }
        return Optional.of(timedOut ? Failure.TIMEOUT : Failure.REFUSED);
    }
}
