package io.github.sudoitir.openbroker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.Credentials.BearerToken;
import io.github.sudoitir.openbroker.Credentials.None;
import io.github.sudoitir.openbroker.Credentials.UsernamePassword;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CredentialsTest {

    @Test
    void aPasswordNeverShowsItselfInText() {
        UsernamePassword c = Credentials.usernamePassword("admin", "hunter2");
        assertThat(c.toString()).contains("admin").doesNotContain("hunter2").contains("redacted");
        assertThat(String.valueOf(c.password())).isEqualTo("Secret[redacted]");
        assertThat(c.password().reveal()).isEqualTo("hunter2");
    }

    @Test
    void aBearerTokenNeverShowsItselfInText() {
        Instant expires = Instant.parse("2026-10-27T12:00:00Z");
        BearerToken c = Credentials.bearerToken("eyJhbGciOi.payload.signature", expires);
        assertThat(c.toString())
                .doesNotContain("eyJhbGciOi")
                .doesNotContain("signature")
                .contains("redacted")
                .contains("2026-10-27T12:00:00Z");
        assertThat(c.token().reveal()).isEqualTo("eyJhbGciOi.payload.signature");
        assertThat(c.expiresAt()).isEqualTo(expires);
    }

    @Test
    void noneHasNoSecretAndEqualsItself() {
        assertThat(new None()).isEqualTo(new None());
        assertThat(CredentialSource.none().forPlane(Plane.DATA)).isInstanceOf(None.class);
    }

    @Test
    void aSourceHandsOutTheSameFormForEveryPlane() {
        Credentials account = Credentials.usernamePassword("a", "b");
        CredentialSource source = CredentialSource.of(account);
        assertThat(source.forPlane(Plane.DATA)).isSameAs(account);
        assertThat(source.forPlane(Plane.MANAGEMENT)).isSameAs(account);
    }
}
