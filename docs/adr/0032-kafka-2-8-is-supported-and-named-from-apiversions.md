# ADR-0032: Kafka 2.8 is supported, and a cluster without `metadata.version` is named from `ApiVersions`

- **Status**: accepted
- **Date**: 2026-10-31
- **Deciders**: Mahdi Amirabdollahi
- **Supersedes**: the version floor of [ADR-0031](0031-kafka-3-0-and-later-declared-per-feature.md)

## Context

ADR-0031 set the floor at Kafka 3.0 and told a cluster that reports no `metadata.version` apart by asking it
for `ListTransactions` (KIP-664): an answer meant "at least 3.0", a refusal meant "before 3.0", refuse.
That inference is what refused a real estate: three brokers on Kafka 2.8 (ZooKeeper mode, `SASL_PLAINTEXT`
with `SCRAM-SHA-256`) answered every request the driver uses — topics, partitions, offsets, configs, groups,
ACLs, producer state — and `kcat` and the Kafka CLI read them fully, but `verify` stopped at
`FAILED_PRECONDITION` before describing a single node, so the UI could not even show the cluster's id or mode.

Everything the driver needs exists in 2.8 except the transaction listing: `DescribeTransactions` and
`ListTransactions` arrived in 3.0 with KIP-664. Transactional ids themselves are still listed, from the
coordinator's own requests, and a transaction can still be aborted partition by partition through
`WriteTxnMarkers` and `DescribeProducers`. Only "hanging transactions", which finds open transactions by
listing producers, cannot be found.

The obstacle to supporting 2.8 was naming it. Below 3.3 there is no `metadata.version`; the Admin API exposes
no API-version table; and ADR-0031 rejected the internal Kafka classes that do, because they change between
releases. So a 2.8 cluster was indistinguishable from a 2.6 one, and the floor could not be lowered honestly.

## Decision

**The floor is Kafka 2.8.** A cluster that serves `DescribeProducers` (2.8) is supported; one that serves
neither it nor `ListTransactions` is refused as before, naming that it is older than 2.8.

**A cluster without `metadata.version` is named from its `ApiVersions` table, read from the wire.** The probe
(`ApiVersions`) opens the socket, sends the v0 `ApiVersions` request and reads the key ranges, in about forty
lines that depend on no Kafka class — the shape has been fixed by the protocol since Kafka 0.10, and Kafka
answers it before it asks for credentials, so no secret is involved. It runs only when `metadata.version` is
absent, only against the bootstrap servers, and only within the caller's remaining deadline. A probe that gets
no answer names nothing: the cluster is then neither supported nor refused on a guess.

**What a 2.8 cluster lacks is declared, never emulated.** `kafka.transaction-listing` is unavailable with its
reason ("needs Kafka 3.0 or later"), `kafka.hanging-transaction` is not declared as a kind, and reading it
says why. Everything else — the quorum (ZooKeeper has none), consumer-protocol groups, share groups, streams
groups, fenced brokers, eligible leaders — is already declared per feature by ADR-0031 and is unchanged.

## Consequences

- A Kafka 2.8 estate connects with its gaps stated; the check reports its cluster id, its nodes and its mode.
- The release of such a cluster reads "at least 2.8": the table says where it starts, not which patch it is.
- The driver sends one request outside `kafka-clients` per connection, on clusters below 3.3 only.
- The test matrix swaps its below-floor leg to Confluent Platform 6.0 (Kafka 2.6) and adds a 2.8 leg.
- Kafka 2.8 is out of support upstream (3.0 superseded it in September 2021, and 2.8 gets no fixes); supporting
  it costs the declared gaps above and nothing else, because the 4.x client speaks to it fully.

## Alternatives considered

- **Keep the 3.0 floor.** It refused clusters the client reads perfectly, for a limit of ours.
- **Infer the floor from refusals** (`UpdateFeatures` ≥ 2.7, `DescribeCluster` ≥ 2.4). Approximate by
  construction: a 2.7 cluster passes the first marker and then fails at runtime on `DescribeProducers`.
- **Use Kafka's internal `ApiKeys`.** Rejected by ADR-0031 and still right: it breaks on upgrade, and the
  v0 request is smaller than the workaround.
- **Emulate the transaction listing on 2.8** by combining `ListGroups` and `DescribeGroups`. It cannot see a
  producer that never joined a group, so it would report a partial answer as a complete one.
