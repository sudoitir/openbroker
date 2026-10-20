package io.github.sudoitir.openbroker;

/** What a consumer can know about a system before connecting: the system and its options. */
public record SystemDescriptor(BrokerSystem system, ConnectionSchema schema) {}
