# ADR-0006: One message model for every system

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

Browsing, copying and relaying messages between systems needs a model that loses nothing within one
system and maps cleanly across them, without Java deserialization or whole bodies in memory.

## Decision

- `MessageHeaders`: id, correlation id, reply-to, content type, type, priority, expiry, durability,
  produced and deliver-at times, delivery count, `key` (partitioning and compaction), `groupId` (the
  ordering group), `deduplicationId` and `originId` (the id a copy came from).
- Properties are a map of a sealed `PropertyValue` (boolean, integer types, floats, string, bytes).
- `MessageBody` streams (`openStream()`), may cache bytes, knows its size and can be a tombstone. Large
  bodies spool to disk.
- Truncation is stated per part; system-specific detail goes in namespaced `extensions` (`jms`,
  `artemis`).
- `OutboundMessage.copyOf` copies losslessly within one system. Provenance properties and dedup-id
  prefixes are the caller's.
- Filters are a `MessageFilter(dialect, expression)`, parsed by the system's own grammar before use;
  `openbroker.match` is the portable dialect.
- The transport (`Receiver`, `Sender`, `Disposition` = accept, release, reject) is `@Incubating` until
  the messaging client change.

## Consequences

- Relays and browsers are written once. No message body is ever deserialized as a Java object.
- The headers are fixed now so the messaging client never breaks them.

## Alternatives considered

- **JMS `Message`.** JMS-shaped, no key, no tombstone, Java serialization for object messages.
