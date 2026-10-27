# ADR-0024: Kinds carry a label

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

A kind has an id (`<system>.<name>`) and no name for a person. A console that lists a system's
resources had to derive a heading from the id, and "group-topic" has no good derived heading
("Group on topic"). Deriving it also puts naming policy in every consumer, differently each time.

## Decision

- `Kind` gains a `label`, `KindLabel(String singular, String plural)` in `model`. Both are non-blank
  and in sentence case ("Group on topic", "Groups on topics").
- Every driver fills the label of every kind it offers. The TCK checks that each label is present.
- Consumers show the label and stop deriving names from ids.

## Consequences

- The names a person reads are decided once, by the driver that knows the system.
- Adopters that construct a `Kind` pass the label as its second argument; this is a breaking change.
- Labels are English. Translation is a consumer's concern, keyed by kind id.

## Alternatives considered

- **Derive the label from the id.** Wrong for compound names and a policy every consumer repeats.
- **A single string.** Pluralising "Group on topic" is a language problem the driver should settle.
- **A lookup in the consumer.** Every consumer would redo it for every driver.
