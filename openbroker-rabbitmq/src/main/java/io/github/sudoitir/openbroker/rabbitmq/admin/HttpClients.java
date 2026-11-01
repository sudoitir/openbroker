package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

/**
 * One {@link HttpClient} per TLS material id, shared through {@link DriverContext#shared}: no redirects, HTTP/1.1, the
 * connect timeout from the call options.
 */
final class HttpClients {

    private HttpClients() {}

    static HttpClient forTls(DriverContext context, Optional<TlsMaterial> tls, Duration connectTimeout) {
        throw RabbitMqErrors.unimplemented("HttpClients.forTls");
    }
}
