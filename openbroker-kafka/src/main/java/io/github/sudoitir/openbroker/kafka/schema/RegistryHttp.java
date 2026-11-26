package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * The HTTP call a Confluent-compatible registry speaks. Hostname verification stays on, redirects are never followed,
 * and a body past {@link #CAP} bytes is aborted rather than buffered.
 */
final class RegistryHttp implements AutoCloseable {

    static final int CAP = 1024 * 1024;

    static final Duration CONNECT = Duration.ofSeconds(5);
    static final Duration REQUEST = Duration.ofSeconds(15);

    private static final System.Logger LOG = System.getLogger(RegistryHttp.class.getName());

    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(CONNECT)
            .build();

    /** One answer. The body is at most {@link #CAP} bytes and has already been read. */
    record Reply(int status, byte[] body) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Reply(int status, byte[] body)
                    && this.status == status
                    && Arrays.equals(this.body, body);
        }

        @Override
        public int hashCode() {
            return 31 * status + Arrays.hashCode(body);
        }

        @Override
        public String toString() {
            return "Reply[status=" + status + ", body=" + Arrays.toString(body) + "]";
        }
    }

    Reply exchange(String method, URI uri, byte[] body, Redactor redactor, boolean write, Duration timeout) {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/json");
        if (body != null) {
            request.header("Content-Type", "application/vnd.schemaregistry.v1+json");
        }
        redactor.authorization().ifPresent(header -> request.header("Authorization", header));
        request.method(
                method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        try {
            HttpResponse<byte[]> response = http.send(request.build(), responseInfo -> new Capped());
            return new Reply(response.statusCode(), response.body());
        } catch (HttpTimeoutException _) {
            throw Deadlines.exceeded(null, write ? Dispatch.IN_DOUBT : Dispatch.NOT_SENT);
        } catch (IOException e) {
            throw transport(e, redactor, write);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Deadlines.cancelled(null, e);
        } catch (ResponseTooLarge _) {
            throw failed(
                    Status.RESOURCE_EXHAUSTED,
                    ErrorReasons.RESPONSE_TOO_LARGE,
                    write ? Dispatch.IN_DOUBT : Dispatch.REJECTED,
                    redactor,
                    "The registry's response was larger than " + CAP + " bytes.");
        }
    }

    private static BrokerException transport(IOException failure, Redactor redactor, boolean write) {
        Throwable cause = failure;
        if (failure.getCause() instanceof ResponseTooLarge) {
            throw failed(
                    Status.RESOURCE_EXHAUSTED,
                    ErrorReasons.RESPONSE_TOO_LARGE,
                    write ? Dispatch.IN_DOUBT : Dispatch.REJECTED,
                    redactor,
                    "The registry's response was larger than " + CAP + " bytes.");
        }
        String detail = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        String message = redactor.apply("Could not reach the schema registry: " + detail);
        if (redactor.leaks(String.valueOf(failure))) {
            cause = null;
        }
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(ErrorReasons.UNREACHABLE),
                write ? Dispatch.IN_DOUBT : Dispatch.NOT_SENT,
                null,
                message,
                cause);
    }

    static BrokerException failed(Status status, String reason, Dispatch dispatch, Redactor redactor, String message) {
        return new BrokerException(status, ErrorInfo.of(reason), dispatch, null, redactor.apply(message));
    }

    /** Logs a failure. The line is redacted before it reaches the logger. */
    static void logFailure(Redactor redactor, String detail) {
        LOG.log(System.Logger.Level.WARNING, "Schema registry call failed: " + redactor.apply(detail));
    }

    @Override
    public void close() {
        http.close();
    }

    /** Strips a registry credential from a message. A password is stripped where it stands as its own token, at any length. */
    static final class Redactor {

        private final String pair;
        private final String password;
        private final String token;

        private Redactor(String pair, String password, String token) {
            this.pair = pair;
            this.password = password;
            this.token = token;
        }

        static Redactor none() {
            return new Redactor("", "", "");
        }

        /** {@code user:password}. A value with no colon is refused by the caller before this is built. */
        static Redactor of(String user, String password) {
            String pair = user + ":" + password;
            String token = Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
            return new Redactor(pair, password, token);
        }

        Optional<String> authorization() {
            return token.isEmpty() ? Optional.empty() : Optional.of("Basic " + token);
        }

        String apply(String message) {
            if (message == null) {
                return "";
            }
            String out = message;
            if (!pair.isEmpty()) {
                out = out.replace(pair, "****");
            }
            out = stripToken(out, password);
            if (!token.isEmpty()) {
                out = out.replace(token, "****");
            }
            return out;
        }

        boolean leaks(String message) {
            if (message == null) {
                return false;
            }
            return (!pair.isEmpty() && message.contains(pair))
                    || containsToken(message, password)
                    || (!token.isEmpty() && message.contains(token));
        }

        /** The password at any length, only where it stands as its own token, so a short one does not eat ordinary words. */
        private static String stripToken(String message, String secret) {
            if (secret.isEmpty()) {
                return message;
            }
            StringBuilder out = new StringBuilder();
            int i = 0;
            while (i < message.length()) {
                if (message.startsWith(secret, i) && tokenBounded(message, i, secret.length())) {
                    out.append("****");
                    i += secret.length();
                } else {
                    out.append(message.charAt(i));
                    i++;
                }
            }
            return out.toString();
        }

        private static boolean containsToken(String message, String secret) {
            if (secret.isEmpty()) {
                return false;
            }
            int from = 0;
            while (from <= message.length() - secret.length()) {
                int at = message.indexOf(secret, from);
                if (at < 0) {
                    return false;
                }
                if (tokenBounded(message, at, secret.length())) {
                    return true;
                }
                from = at + 1;
            }
            return false;
        }

        private static boolean tokenBounded(String message, int start, int length) {
            boolean left = start == 0 || !isTokenChar(message.charAt(start - 1));
            int end = start + length;
            boolean right = end >= message.length() || !isTokenChar(message.charAt(end));
            return left && right;
        }

        private static boolean isTokenChar(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }
    }

    private static final class ResponseTooLarge extends RuntimeException {}

    /** Stops reading once {@link #CAP} bytes have arrived, so a huge body is never buffered. */
    private static final class Capped implements HttpResponse.BodySubscriber<byte[]> {

        private final CompletableFuture<byte[]> done = new CompletableFuture<>();
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        private Flow.Subscription subscription;

        @Override
        public CompletionStage<byte[]> getBody() {
            return done;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                int remaining = buffer.remaining();
                if (body.size() + remaining > CAP) {
                    subscription.cancel();
                    done.completeExceptionally(new ResponseTooLarge());
                    return;
                }
                byte[] chunk = new byte[remaining];
                buffer.get(chunk);
                body.write(chunk, 0, chunk.length);
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            done.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            done.complete(body.toByteArray());
        }
    }
}
