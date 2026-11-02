package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ListingTest {

    @Test
    void aServerCursorRoundTrips() {
        String token = Listing.token(new Listing.Cursor(true, 3));
        assertThat(new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8))
                .isEqualTo("p:3");
        Listing.Cursor back = Listing.decode(Optional.of(token));
        assertThat(back.server()).isTrue();
        assertThat(back.page()).isEqualTo(3);
        assertThat(back.offset()).isZero();
    }

    @Test
    void aClientCursorRoundTrips() {
        Listing.Cursor back = Listing.decode(Optional.of(Listing.token(new Listing.Cursor(false, 40))));
        assertThat(back.server()).isFalse();
        assertThat(back.offset()).isEqualTo(40);
        assertThat(back.page()).isEqualTo(1);
    }

    @Test
    void noTokenStartsAtTheBeginning() {
        Listing.Cursor start = Listing.decode(Optional.empty());
        assertThat(start.page()).isEqualTo(1);
        assertThat(start.offset()).isZero();
    }

    @Test
    void aTokenThatIsNotOursIsInvalid() {
        for (String bad : new String[] {
            "!!!",
            Base64.getUrlEncoder().encodeToString("x:1".getBytes(StandardCharsets.UTF_8)),
            Base64.getUrlEncoder().encodeToString("p:0".getBytes(StandardCharsets.UTF_8)),
            Base64.getUrlEncoder().encodeToString("o:x".getBytes(StandardCharsets.UTF_8))
        }) {
            Optional<String> token = Optional.of(bad);
            assertThatThrownBy(() -> Listing.decode(token))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
        }
    }
}
