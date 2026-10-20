package io.github.sudoitir.openbroker.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

/** An immutable byte sequence with value equality. */
public final class Blob {

    private static final Blob EMPTY = new Blob(new byte[0]);

    private final byte[] bytes;

    private Blob(byte[] bytes) {
        this.bytes = bytes;
    }

    public static Blob of(byte[] bytes) {
        return bytes.length == 0 ? EMPTY : new Blob(bytes.clone());
    }

    public static Blob utf8(String text) {
        return of(text.getBytes(StandardCharsets.UTF_8));
    }

    public static Blob empty() {
        return EMPTY;
    }

    public byte[] toByteArray() {
        return bytes.clone();
    }

    public int size() {
        return bytes.length;
    }

    public String asUtf8() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Blob other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return bytes.length <= 32
                ? "Blob[" + HexFormat.of().formatHex(bytes) + "]"
                : "Blob[" + bytes.length + " bytes]";
    }
}
