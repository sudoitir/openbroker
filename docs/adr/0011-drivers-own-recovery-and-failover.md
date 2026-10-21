# ADR-0011: Drivers own recovery and failover

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

Artemis Core links connected once with `reconnectAttempts=0`, and a release settled every unsettled
message of the receiver. A broker restart or a failover ended the link. Kafka, Pulsar and NATS clients
all own their reconnect, and a caller needs the same contract on every driver: what is lost, what is
duplicated, and when to give up.

## Decision

- A link has a `Recovery(Backoff, giveUpAfter)` and a state: `OPEN`, `RECOVERING`, `FAILED`,
  `CLOSED`. `Transport.watch(LinkListener)` delivers `LinkEvent`s, serial per link.
- On loss the SDK's supervisor reopens the link on its node. If the node has an HA group
  (`Membership.group`, with peer endpoints taken from the `ClusterSpec`), it reopens on the group's
  serving node. It ends `OPEN` with `servedBy`, or `FAILED` after `giveUpAfter`.
- Every reopen is a data-plane control call admitted by the `CallInterceptor` (operation
  `link.recover`). `CredentialSource` and `TlsProvider` are asked again at every open and recovery.
- The loss/duplicate rules per guarantee are the contract, in the README and the `messaging` package
  documentation. After recovery, unconfirmed `AT_LEAST_ONCE` sends are resent in order
  (`resent=true`); `AT_MOST_ONCE` sends are never resent (`UNAVAILABLE` / `IN_DOUBT`); settling a
  message received before recovery fails `FAILED_PRECONDITION` / `DELIVERY_STALE`; a transacted
  commit fails `ABORTED` / `TRANSACTION_ROLLED_BACK`.
- The broker client's own failover is not used.

## Consequences

- One recovery contract for every driver, tested by chaos cases ([ADR-0017](0017-chaos-is-part-of-the-tck.md)).
- A message held by a dead session is redelivered only after the broker's connection TTL on a
  half-open link. That latency is documented and driven by `Timeouts`.
- Without a readable `Membership`, recovery reopens the same node only.
- A resend can duplicate unless the sender is idempotent; the dedupe window is a count on Artemis and
  is reported by `Sender.deduplication()`.

## Alternatives considered

- **Artemis `ha=true` with `reconnectAttempts` and session re-attach.** It dials topology-announced
  connectors, unreachable behind NAT, proxies or Toxiproxy. Re-attach hides the failure, so a
  non-transacted send can be lost silently. Its connector matching is absent on the oldest supported
  broker (2.33). The contract has to be the same on every driver, and Kafka, Pulsar and NATS own their
  reconnect.
