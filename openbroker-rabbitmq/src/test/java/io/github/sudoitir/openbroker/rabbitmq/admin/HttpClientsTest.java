package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.TlsMaterial;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

class HttpClientsTest {

    private final FakeManagement.TestContext context = new FakeManagement.TestContext(ConnectOptions.defaults());

    @Test
    void oneClientPerMaterialAndTimeoutNeverFollowingRedirects() throws Exception {
        HttpClient plain = HttpClients.forTls(context, Optional.empty(), Duration.ofSeconds(3));
        assertThat(HttpClients.forTls(context, Optional.empty(), Duration.ofSeconds(3)))
                .isSameAs(plain);
        assertThat(plain.followRedirects()).isEqualTo(HttpClient.Redirect.NEVER);
        assertThat(plain.version()).isEqualTo(HttpClient.Version.HTTP_1_1);
        assertThat(plain.connectTimeout()).contains(Duration.ofSeconds(3));
        assertThat(HttpClients.forTls(context, Optional.empty(), Duration.ofSeconds(4)))
                .isNotSameAs(plain);
    }

    @Test
    void tlsMaterialSetsProtocolsCiphersAndHostnameVerification() throws Exception {
        SSLContext ssl = SSLContext.getDefault();
        TlsMaterial first = new TlsMaterial("m1", ssl, true, List.of("TLSv1.3"), List.of("TLS_AES_128_GCM_SHA256"));
        HttpClient client = HttpClients.forTls(context, Optional.of(first), Duration.ofSeconds(3));
        assertThat(client.sslParameters().getProtocols()).containsExactly("TLSv1.3");
        assertThat(client.sslParameters().getCipherSuites()).containsExactly("TLS_AES_128_GCM_SHA256");
        assertThat(client.sslParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        TlsMaterial reloaded = TlsMaterial.of("m1", ssl);
        HttpClient again = HttpClients.forTls(context, Optional.of(reloaded), Duration.ofSeconds(4));
        assertThat(again).isNotSameAs(client);
        TlsMaterial lax = new TlsMaterial("m2", ssl, false, List.of(), List.of());
        assertThat(HttpClients.forTls(context, Optional.of(lax), Duration.ofSeconds(3))
                        .sslParameters()
                        .getEndpointIdentificationAlgorithm())
                .isNull();
    }
}
