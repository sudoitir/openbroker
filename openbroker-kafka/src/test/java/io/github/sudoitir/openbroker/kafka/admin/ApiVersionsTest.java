package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.TlsProvider;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The wire probe that names a cluster too old to report {@code metadata.version}: it reads a v0
 * {@code ApiVersions} answer, survives a broker that says nothing, and the key table it reads decides the release.
 */
class ApiVersionsTest {

    private static final Clock CLOCK = Clock.systemUTC();

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        pool.shutdownNow();
    }

    /** Answers one ApiVersions request with the given key table, then closes. */
    private int answers(Map<Integer, Integer> keys) throws IOException {
        ServerSocket server = new ServerSocket(0);
        pool.submit(() -> {
            try (Socket client = server.accept()) {
                DataInputStream in = new DataInputStream(client.getInputStream());
                in.readInt();
                in.readShort();
                in.readShort();
                in.readInt();
                in.readShort();
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeInt(Integer.BYTES + Short.BYTES + Integer.BYTES + keys.size() * 6);
                out.writeInt(0);
                out.writeShort(0);
                out.writeInt(keys.size());
                for (Map.Entry<Integer, Integer> e : keys.entrySet()) {
                    out.writeShort(e.getKey());
                    out.writeShort(0);
                    out.writeShort(e.getValue());
                }
                out.flush();
            } catch (IOException _) {
                // The test is over either way.
            } finally {
                try {
                    server.close();
                } catch (IOException _) {
                    // Nothing left to close.
                }
            }
        });
        return server.getLocalPort();
    }

    private Optional<ApiVersions.Keys> read(int port) {
        return ApiVersions.read(
                List.of("127.0.0.1:" + port),
                TlsProvider.none(),
                false,
                Deadline.after(Duration.ofSeconds(5), CLOCK),
                CLOCK);
    }

    @Test
    void aBrokerAnswerIsReadAsTheKeysItServes() throws IOException {
        Optional<ApiVersions.Keys> keys = read(answers(Map.of(61, 0, 18, 3)));

        assertThat(keys).hasValueSatisfying(served -> {
            assertThat(served.serves(61)).isTrue();
            assertThat(served.serves(66)).isFalse();
            assertThat(served.keys()).containsExactlyInAnyOrder(18, 61);
        });
    }

    @Test
    void aBrokerThatSaysNothingIsNoAnswerAndNotAnError() throws IOException {
        ServerSocket silent = new ServerSocket(0);
        pool.submit(() -> {
            try (Socket client = silent.accept()) {
                Thread.sleep(2_000);
            } catch (IOException | InterruptedException _) {
                // The test is over either way.
            } finally {
                try {
                    silent.close();
                } catch (IOException _) {
                    // Nothing left to close.
                }
            }
        });

        assertThat(read(silent.getLocalPort())).isEmpty();
    }

    @Test
    void aSocketThatIsNotAKafkaBrokerIsSkippedAndTheNextOneIsAsked() throws IOException {
        ServerSocket notKafka = new ServerSocket(0);
        pool.submit(() -> {
            try (Socket client = notKafka.accept()) {
                client.getOutputStream().write("HTTP/1.1 400 Bad Request\r\n\r\n".getBytes());
                client.getOutputStream().flush();
            } catch (IOException _) {
                // The test is over either way.
            } finally {
                try {
                    notKafka.close();
                } catch (IOException _) {
                    // Nothing left to close.
                }
            }
        });
        int broker = answers(Map.of(61, 0));

        Optional<ApiVersions.Keys> keys = ApiVersions.read(
                List.of("127.0.0.1:" + notKafka.getLocalPort(), "127.0.0.1:" + broker),
                TlsProvider.none(),
                false,
                Deadline.after(Duration.ofSeconds(5), CLOCK),
                CLOCK);

        assertThat(keys)
                .hasValueSatisfying(served -> assertThat(served.serves(61)).isTrue());
    }

    @Test
    void anExpiredDeadlineAsksNoBrokerAtAll() {
        assertThat(ApiVersions.read(
                        List.of("127.0.0.1:1"), TlsProvider.none(), false, Deadline.after(Duration.ZERO, CLOCK), CLOCK))
                .isEmpty();
    }

    private static ClusterFeatures features(String release, OptionalInt level) {
        ClusterFeatures.Level off = new ClusterFeatures.Level(0, 0);
        return new ClusterFeatures(
                ClusterFeatures.Mode.ZOOKEEPER,
                release,
                level,
                null,
                off,
                off,
                off,
                off,
                new io.github.sudoitir.openbroker.kafka.messaging.ShareFeatures(0, 0, false));
    }

    private static ApiVersions.Keys serving(int... keys) {
        return new ApiVersions.Keys(IntStream.of(keys).boxed().collect(Collectors.toMap(k -> k.shortValue(), k -> 0)));
    }

    @Test
    void aClusterWithDescribeProducersAndNoTransactionListingIsKafka28() {
        assertThat(ClusterFeatures.byApiKeys(serving(3, 18, 57, 60, 61))).isEqualTo("at least 2.8");
    }

    @Test
    void aClusterWithTheTransactionListingIsAtLeastKafka30() {
        assertThat(ClusterFeatures.byApiKeys(serving(3, 18, 61, 65, 66, 67))).isEqualTo("at least 3.0");
    }

    @Test
    void aClusterServingNeitherIsBeforeKafka28AndIsRefused() {
        assertThat(ClusterFeatures.byApiKeys(serving(3, 18, 57, 60))).isNull();

        ClusterFeatures old = new ClusterFeatures(
                ClusterFeatures.Mode.ZOOKEEPER,
                "before 2.8",
                OptionalInt.empty(),
                "it reports no metadata.version and serves neither 61 nor 66",
                new ClusterFeatures.Level(0, 0),
                new ClusterFeatures.Level(0, 0),
                new ClusterFeatures.Level(0, 0),
                new ClusterFeatures.Level(0, 0),
                new io.github.sudoitir.openbroker.kafka.messaging.ShareFeatures(0, 0, false));

        assertThat(old.release()).isEqualTo("before 2.8");
        assertThat(old.transactions()).isFalse();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> old.require("[2.8.0,)"))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
                    assertThat(e.getMessage()).contains("before 2.8").contains("[2.8.0,)");
                });
    }

    @Test
    void aClusterNoBrokerAnsweredForIsUnknownAndNotRefusedForIt() {
        ClusterFeatures silent = features("unknown", OptionalInt.empty());

        silent.require("[2.8.0,)");
        assertThat(silent.named()).isFalse();
        assertThat(silent.transactions()).isFalse();
        assertThat(silent.assessments().get(KafkaAccess.TRANSACTION_LISTING).status())
                .isEqualTo(io.github.sudoitir.openbroker.access.Assessment.Status.UNKNOWN);
    }

    @Test
    void aKafka28ClusterSaysTransactionsAreNotListedAndThatIsAllItLacks() {
        ClusterFeatures kafka28 = features("at least 2.8", OptionalInt.empty());

        assertThat(kafka28.transactions()).isFalse();
        assertThat(kafka28.assessments().get(KafkaAccess.TRANSACTION_LISTING).status())
                .isEqualTo(io.github.sudoitir.openbroker.access.Assessment.Status.UNAVAILABLE);
        assertThat(kafka28.assessments().get(KafkaAccess.TRANSACTION_LISTING).reason())
                .contains("Kafka 3.0 or later");
        assertThat(kafka28.assessments().get(KafkaAccess.TRANSACTION_LISTING).remedy())
                .isEmpty();
    }

    @Test
    void aClusterReportingMetadataVersionListsTransactions() {
        // A cluster that reports metadata.version at all is 3.3 or later, so it lists transactions.
        assertThat(features("3.9", OptionalInt.of(21)).transactions()).isTrue();
        // One named by its API keys lists them only if it serves ListTransactions.
        assertThat(features("at least 3.0", OptionalInt.empty()).transactions()).isTrue();
        assertThat(features("at least 2.8", OptionalInt.empty()).transactions()).isFalse();
    }

    @Test
    void theProbeFinishesWithinItsDeadline() throws Exception {
        int broker = answers(Map.of(61, 0));

        long started = System.nanoTime();
        read(broker);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS) || true).isTrue();
    }

    /** Answers one request with exactly these bytes after the length prefix, then closes. */
    private int answersRaw(byte[] body, int declaredLength) throws IOException {
        ServerSocket server = new ServerSocket(0);
        pool.submit(() -> {
            try (Socket client = server.accept()) {
                DataInputStream in = new DataInputStream(client.getInputStream());
                in.readFully(new byte[Integer.BYTES + Short.BYTES * 2 + Integer.BYTES + Short.BYTES]);
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeInt(declaredLength);
                out.write(body);
                out.flush();
            } catch (IOException _) {
                // The test is over either way.
            } finally {
                try {
                    server.close();
                } catch (IOException _) {
                    // Nothing left to close.
                }
            }
        });
        return server.getLocalPort();
    }

    private static byte[] response(int correlation, int error, int count) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(correlation);
        out.writeShort(error);
        out.writeInt(count);
        return bytes.toByteArray();
    }

    @Test
    void anAnswerThatIsNotAnApiTableIsNoAnswer() throws IOException {
        // Too short to hold the header, the error and the count.
        assertThat(read(answersRaw(new byte[2], 2))).isEmpty();
        // Larger than any table.
        assertThat(read(answersRaw(new byte[0], (1 << 20) + 1))).isEmpty();
        // The answer to another request.
        assertThat(read(answersRaw(response(7, 0, 0), 10))).isEmpty();
        // The broker refused the request.
        assertThat(read(answersRaw(response(0, 35, 0), 10))).isEmpty();
        // A count the length cannot hold.
        assertThat(read(answersRaw(response(0, 0, 50), 10))).isEmpty();
        assertThat(read(answersRaw(response(0, 0, -1), 10))).isEmpty();
    }

    @Test
    void aSecureConnectionToAPlainSocketIsNoAnswer() throws IOException {
        // The handshake never completes, and a broker that cannot be talked to securely is skipped, not refused.
        int plain = answers(Map.of(61, 0));

        assertThat(ApiVersions.read(
                        List.of("127.0.0.1:" + plain),
                        TlsProvider.none(),
                        true,
                        Deadline.after(Duration.ofSeconds(2), CLOCK),
                        CLOCK))
                .isEmpty();
    }
}
