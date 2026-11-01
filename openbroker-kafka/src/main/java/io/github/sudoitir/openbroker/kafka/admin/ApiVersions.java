package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.TlsProvider;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * The API keys a cluster's brokers serve, read from the wire.
 *
 * <p>A cluster below Kafka 3.3 cannot be named through {@code Admin}: it reports no {@code metadata.version}, and the
 * API version table is behind internal Kafka classes that change between releases (ADR-0031). Yet that table is what
 * says whether such a cluster is one this driver supports, so it is read here, in the v0 shape of
 * {@code ApiVersions} that the protocol has fixed since Kafka 0.10. Kafka answers that request before it asks for
 * credentials, so the probe needs none.
 *
 * <p>This is not a general release oracle. It answers only what {@code metadata.version} cannot, and only with keys
 * whose release is a protocol fact (ADR-0032): {@code DescribeProducers} arrived in Kafka 2.8 and {@code
 * ListTransactions} in 3.0, the two halves of KIP-664.
 */
final class ApiVersions {

    /** {@code ApiVersions}, whose v0 every broker from Kafka 0.10 on answers. */
    private static final short API_VERSIONS = 18;

    private static final short V0 = 0;

    /** A null client id in the v0 request header. */
    private static final short NO_CLIENT_ID = -1;

    /** A response shorter than the header plus the error and the count of keys is not an answer. */
    private static final int MIN_RESPONSE = Integer.BYTES + Short.BYTES + Integer.BYTES;

    /** The largest answer that is still an API table; anything bigger is not one. */
    private static final int MAX_RESPONSE = 1 << 20;

    private ApiVersions() {}

    /** One broker's answer, or empty when none of {@code addresses} answered within {@code limit}. */
    static Optional<Keys> read(
            Iterable<String> addresses, TlsProvider tls, boolean secure, Deadline limit, Clock clock) {
        Optional<TlsMaterial> material = secure ? tls.current() : Optional.empty();
        for (String address : addresses) {
            if (limit.expired(clock)) {
                return Optional.empty();
            }
            try {
                return Optional.of(read(address, material, secure, limit, clock));
            } catch (IOException | RuntimeException _) {
                // Not a broker that speaks the protocol, or not one that answered in time: the next may.
            }
        }
        return Optional.empty();
    }

    private static Keys read(
            String address, Optional<TlsMaterial> material, boolean secure, Deadline limit, Clock clock)
            throws IOException {
        try (Socket socket = connect(address, material, secure, limit, clock)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(Short.BYTES * 2 + Integer.BYTES + Short.BYTES);
            out.writeShort(API_VERSIONS);
            out.writeShort(V0);
            out.writeInt(0);
            out.writeShort(NO_CLIENT_ID);
            out.flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int length = in.readInt();
            if (length < MIN_RESPONSE || length > MAX_RESPONSE) {
                throw new IOException("Not an ApiVersions response: " + length + " bytes");
            }
            if (in.readInt() != 0) {
                throw new IOException("Not the answer to this request");
            }
            short error = in.readShort();
            if (error != 0) {
                throw new IOException("The broker refused ApiVersions with error " + error);
            }
            int count = in.readInt();
            if (count < 0 || count > (length - MIN_RESPONSE) / (Short.BYTES * 3)) {
                throw new IOException("An ApiVersions response cannot list " + count + " keys");
            }
            Map<Short, Integer> newest = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                short key = in.readShort();
                in.readShort();
                newest.put(key, (int) in.readShort());
            }
            return new Keys(newest);
        } catch (EOFException _) {
            throw new IOException("The broker closed the connection before answering");
        }
    }

    private static Socket connect(
            String address, Optional<TlsMaterial> material, boolean secure, Deadline limit, Clock clock)
            throws IOException {
        String host = Sockets.host(address);
        int port = Sockets.port(address);
        int millis = (int) Math.clamp(limit.remaining(clock).toMillis(), 1, Integer.MAX_VALUE);
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), millis);
        socket.setSoTimeout(millis);
        if (!secure) {
            return socket;
        }
        SSLSocket tls = (SSLSocket) context(material).getSocketFactory().createSocket(socket, host, port, true);
        SSLParameters parameters = tls.getSSLParameters();
        // The same host-name check Kafka's client makes by default: a probe must not be laxer than the client.
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        tls.setSSLParameters(parameters);
        tls.startHandshake();
        return tls;
    }

    private static SSLContext context(Optional<TlsMaterial> material) {
        if (material.isPresent()) {
            return material.get().context();
        }
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("The JVM has no default TLS context", e);
        }
    }

    /** What one broker serves: the newest version of each API key, by key. */
    record Keys(Map<Short, Integer> newest) {

        Keys {
            newest = Map.copyOf(newest);
        }

        /** Whether the broker serves {@code key} at any version. */
        boolean serves(int key) {
            return newest.containsKey((short) key);
        }

        /** The keys the broker serves. */
        Set<Integer> keys() {
            return newest.keySet().stream()
                    .map(Short::intValue)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
        }
    }
}
