/**
 * The Kafka driver's schema registry: subjects, versions, compatibility, and decoding a payload that
 * carries a schema id.
 *
 * <p>This is native Kafka behaviour, so it is reached through {@code unwrap} rather than being part of
 * the neutral facets every driver must offer. A broker system with no schema registry has nothing to
 * put here, and is not made to pretend it does.
 *
 * <p>The wiring to a particular registry product (a Confluent-compatible endpoint, or a cloud
 * registry) lives in this package and is chosen by {@code registry.kind} on the connection.
 */
package io.github.sudoitir.openbroker.kafka.schema;
