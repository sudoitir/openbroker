package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ManagementPathsTest {

    @Test
    void theDefaultVirtualHostSlashIsEncoded() {
        assertThat(ManagementPaths.segment("/")).isEqualTo("%2F");
    }

    @Test
    void unreservedCharactersStayAsTheyAre() {
        assertThat(ManagementPaths.segment("Az09-._~")).isEqualTo("Az09-._~");
    }

    @Test
    void reservedAndSpecialCharactersAreEncoded() {
        assertThat(ManagementPaths.segment("a/b%c#d e?f&g")).isEqualTo("a%2Fb%25c%23d%20e%3Ff%26g");
    }

    @Test
    void unicodeIsEncodedAsUtf8Bytes() {
        assertThat(ManagementPaths.segment("café")).isEqualTo("caf%C3%A9");
        assertThat(ManagementPaths.segment("日")).isEqualTo("%E6%97%A5");
    }

    @Test
    void theEmptyNameIsAnEmptySegment() {
        assertThat(ManagementPaths.segment("")).isEmpty();
    }

    @Test
    void theDefaultExchangeIsNamedAmqDefaultInIdsAndEmptyOnTheServer() {
        assertThat(ManagementPaths.idName("")).isEqualTo("amq.default");
        assertThat(ManagementPaths.idName("amq.direct")).isEqualTo("amq.direct");
        assertThat(ManagementPaths.serverName("amq.default")).isEmpty();
        assertThat(ManagementPaths.serverName("orders")).isEqualTo("orders");
    }
}
