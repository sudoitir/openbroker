package io.github.sudoitir.openbroker.rabbitmq.admin;

import java.nio.charset.StandardCharsets;

/**
 * Percent-encodes virtual hosts and names as path segments ({@code /} becomes {@code %2F}) and maps the default
 * exchange's empty name to {@code amq.default} and back.
 */
final class ManagementPaths {

    /** The id name of the default exchange, whose server name is empty. */
    static final String DEFAULT_EXCHANGE = "amq.default";

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ManagementPaths() {}

    /** {@code name} as one path segment: everything outside RFC 3986 unreserved characters is encoded. */
    static String segment(String name) {
        StringBuilder out = new StringBuilder(name.length() + 8);
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '-'
                    || c == '.'
                    || c == '_'
                    || c == '~') {
                out.append((char) c);
            } else {
                out.append('%').append(HEX[c >> 4]).append(HEX[c & 0xf]);
            }
        }
        return out.toString();
    }

    /** The id name for a server-side exchange name: {@code ""} becomes {@code amq.default}. */
    static String idName(String serverName) {
        return serverName.isEmpty() ? DEFAULT_EXCHANGE : serverName;
    }

    /** The server-side exchange name for an id name: {@code amq.default} becomes {@code ""}. */
    static String serverName(String idName) {
        return DEFAULT_EXCHANGE.equals(idName) ? "" : idName;
    }
}
