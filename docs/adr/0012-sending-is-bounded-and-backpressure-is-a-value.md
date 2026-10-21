# ADR-0012: Sending is bounded and backpressure is a value

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

An unbounded send queue turns a slow broker into an out-of-memory error in the caller. Kafka's
`send()` blocks for `max.block.ms` and has no non-blocking form; Pulsar's `blockIfQueueFull=false`
throws on every refused message. A relay needs both a blocking send and one that says "full" cheaply.

## Decision

- `SendBuffer(maxInFlight, maxBytes, maxBlock)` bounds messages accepted but not yet confirmed.
- `Sender.offer` never blocks and never throws for a full buffer: it returns `Offer.Accepted`
  (a `CompletionStage<SendReceipt>`) or `Offer.Full(inFlight, bufferedBytes)`.
- `Sender.send` waits up to `maxBlock` for room, then fails `RESOURCE_EXHAUSTED` /
  `SEND_BUFFER_FULL`, request not sent; otherwise it waits for the guarantee's confirmation.
  `flush(timeout)` waits for every accepted message.
- `AT_LEAST_ONCE` resends after recovery ([ADR-0011](0011-drivers-own-recovery-and-failover.md)).
  A resend keeps order only when the sender is idempotent, so `ordering != NONE` with
  `AT_LEAST_ONCE` requires `idempotent=true`, else `INVALID_ARGUMENT`.
- Idempotence is the system's own deduplication, keyed by the `deduplicationId` header; a message
  without one gets a UUIDv7. A system without id deduplication declares `deduplication=false`.
- Non-transacted senders are thread-safe; a transacted one has a single owner.

## Consequences

- Backpressure is visible to the caller and costs no exception on the hot path.
- Memory held by sending is bounded by `maxBytes` per sender.
- Idempotence reaches only as far as the system's dedupe window; a burst larger than Artemis's
  `id-cache-size` between a message and its resend can duplicate.

## Alternatives considered

- **Kafka's blocking-only `send`.** No way to shed or defer without a thread.
- **Pulsar's throwing `sendAsync`.** An exception per message on the hot path.
- **An unbounded queue.** Moves the failure to the heap.
- **A reactive-streams `Publisher`.** A dependency, and with virtual threads `send` already composes.
- **A client-side dedupe cache.** It is not shared across producers or restarts, so it would be false
  comfort.
