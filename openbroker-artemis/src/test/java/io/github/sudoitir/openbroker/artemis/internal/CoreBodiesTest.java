package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Artemis's text body encoding, streamed both ways, against the client's own codec. */
class CoreBodiesTest {

    private static final String TEXT = "plain, accented é, euro €, and an emoji 😀 that needs two chars";

    private static byte[] artemisEncoded(String text) {
        ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(64);
        buffer.writeNullableSimpleString(SimpleString.of(text));
        byte[] out = new byte[buffer.readableBytes()];
        buffer.readBytes(out);
        return out;
    }

    @Test
    void aStreamedTextBodyDecodesToTheSameUtf8AsTheClient() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CoreBodies.TextDecoder decoder = new CoreBodies.TextDecoder(out, Long.MAX_VALUE);
        for (byte b : artemisEncoded(TEXT.repeat(500))) {
            decoder.write(b);
        }
        decoder.close();

        assertThat(out.toString(StandardCharsets.UTF_8)).isEqualTo(TEXT.repeat(500));
    }

    @Test
    void aDecodedTextStopsAtTheLimit() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CoreBodies.TextDecoder decoder = new CoreBodies.TextDecoder(out, 10);
        for (byte b : artemisEncoded("0123456789abcdef")) {
            decoder.write(b);
        }
        decoder.close();

        assertThat(out.toString(StandardCharsets.UTF_8)).isEqualTo("0123456789");
    }

    @Test
    void anEncodedTextIsWhatTheClientWouldWrite(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("body");

        CoreBodies.encodeText(new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8)), file);

        assertThat(Files.readAllBytes(file)).isEqualTo(artemisEncoded(TEXT));
    }

    @Test
    void aCappedReadKeepsTheHeadAndSaysItWasCut() {
        CoreBodies.CappedOutput out = new CoreBodies.CappedOutput(4);

        out.write("abcdef".getBytes(StandardCharsets.UTF_8), 0, 6);

        assertThat(out.toByteArray()).isEqualTo("abcd".getBytes(StandardCharsets.UTF_8));
        assertThat(out.cut()).isTrue();
    }

    @Test
    void onlyValidUtf8IsText() {
        assertThat(CoreBodies.isUtf8("é".getBytes(StandardCharsets.UTF_8))).isTrue();
        assertThat(CoreBodies.isUtf8(new byte[] {(byte) 0xC3, (byte) 0x28})).isFalse();
    }
}
