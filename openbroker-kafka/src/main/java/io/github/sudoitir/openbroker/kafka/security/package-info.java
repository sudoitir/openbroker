/**
 * The pieces that give Kafka clients the connection's credentials and TLS material on every new
 * connection, so a rotation needs no restart. The clients instantiate them by class name; the
 * connection passes what they read in the client configuration.
 */
@NullMarked
package io.github.sudoitir.openbroker.kafka.security;

import org.jspecify.annotations.NullMarked;
