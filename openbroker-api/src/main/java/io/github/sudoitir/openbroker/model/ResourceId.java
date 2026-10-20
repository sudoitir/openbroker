package io.github.sudoitir.openbroker.model;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The logical, cluster-wide identity of a resource: its kind and its path.
 *
 * <p>Its text form ({@link #format()}) is {@code <kind>/<segment>/<segment>...}, each segment
 * percent-encoded with every byte outside the RFC 3986 unreserved set encoded in upper-case hex
 * ({@code artemis.queue/orders%2Feu}). The form is canonical: {@link #parse} accepts only text that
 * {@link #format()} would produce, so equal text means an equal id and the text can be a database key.
 */
public record ResourceId(KindId kind, ResourcePath path) implements Comparable<ResourceId> {

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    public ResourceId {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(path, "path");
    }

    public static ResourceId of(String kind, String... segments) {
        return new ResourceId(KindId.of(kind), ResourcePath.of(segments));
    }

    public String name() {
        return path.name();
    }

    public String format() {
        StringBuilder out = new StringBuilder(kind.value());
        for (String segment : path.segments()) {
            out.append('/');
            encode(segment, out);
        }
        return out.toString();
    }

    /**
     * The id this canonical text names.
     *
     * @throws IllegalArgumentException when the text is not canonical: no segment, an empty segment,
     *     a malformed or lower-case escape, or an escape of a character that needs none
     */
    public static ResourceId parse(String text) {
        Objects.requireNonNull(text, "text");
        String[] parts = text.split("/", -1);
        if (parts.length < 2) {
            throw new IllegalArgumentException("A resource id has a kind and at least one segment: " + text);
        }
        List<String> segments = new ArrayList<>(parts.length - 1);
        for (int i = 1; i < parts.length; i++) {
            String decoded = decode(parts[i], text);
            StringBuilder again = new StringBuilder();
            encode(decoded, again);
            if (!again.toString().equals(parts[i])) {
                throw new IllegalArgumentException("Not the canonical form of a resource id: " + text);
            }
            segments.add(decoded);
        }
        return new ResourceId(KindId.of(parts[0]), new ResourcePath(segments));
    }

    @Override
    public int compareTo(ResourceId other) {
        return format().compareTo(other.format());
    }

    @Override
    public String toString() {
        return format();
    }

    private static void encode(String segment, StringBuilder out) {
        for (byte b : segment.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if (isUnreserved(c)) {
                out.append((char) c);
            } else {
                out.append('%').append(HEX[c >> 4]).append(HEX[c & 0xF]);
            }
        }
    }

    private static String decode(String segment, String text) {
        if (segment.isEmpty()) {
            throw new IllegalArgumentException("A resource id has an empty segment: " + text);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == '%') {
                if (i + 2 >= segment.length()) {
                    throw new IllegalArgumentException("A truncated escape in resource id: " + text);
                }
                int hi = Character.digit(segment.charAt(i + 1), 16);
                int lo = Character.digit(segment.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw new IllegalArgumentException("A malformed escape in resource id: " + text);
                }
                bytes.write((hi << 4) | lo);
                i += 2;
            } else if (c < 0x80) {
                bytes.write(c);
            } else {
                throw new IllegalArgumentException("A resource id is ASCII; encode other characters: " + text);
            }
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private static boolean isUnreserved(int c) {
        return (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '-'
                || c == '.'
                || c == '_'
                || c == '~';
    }
}
