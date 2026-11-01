package io.github.sudoitir.openbroker.rabbitmq.admin;

/**
 * Percent-encodes virtual hosts and names as path segments ({@code /} becomes {@code %2F}) and maps the default
 * exchange's empty name to {@code amq.default} and back.
 */
final class ManagementPaths {

    /** The id name of the default exchange, whose server name is empty. */
    static final String DEFAULT_EXCHANGE = "amq.default";

    private ManagementPaths() {}

    /** {@code name} as one path segment: everything outside RFC 3986 unreserved characters is encoded. */
    static String segment(String name) {
        throw RabbitMqErrors.unimplemented("ManagementPaths.segment");
    }

    /** The id name for a server-side exchange name: {@code ""} becomes {@code amq.default}. */
    static String idName(String serverName) {
        throw RabbitMqErrors.unimplemented("ManagementPaths.idName");
    }

    /** The server-side exchange name for an id name: {@code amq.default} becomes {@code ""}. */
    static String serverName(String idName) {
        throw RabbitMqErrors.unimplemented("ManagementPaths.serverName");
    }
}
