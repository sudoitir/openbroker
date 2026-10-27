package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Credentials.None;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Status;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class AccountsTest {

    @Test
    void aUsernameAndPasswordIsPresented() {
        Credentials account = Credentials.usernamePassword("admin", "secret");
        assertThat(Accounts.account(account)).containsSame((Credentials.UsernamePassword) account);
    }

    @Test
    void noCredentialsMeansAnAnonymousConnection() {
        assertThat(Accounts.account(new None())).isEmpty();
    }

    @Test
    void aBearerTokenIsRefusedNamingTheAcceptedForms() {
        Credentials token = Credentials.bearerToken("abc.def.ghi", Instant.parse("2026-10-27T12:00:00Z"));
        assertThatThrownBy(() -> Accounts.account(token)).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
            assertThat(e.getMessage()).contains("username and password").doesNotContain("abc.def.ghi");
        });
    }
}
