# ADR-0004: Errors carry a status, a reason, and whether the request was sent

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

A caller deciding whether to retry needs more than "it failed": was it refused, did it time out before
leaving, or did it leave and the answer was lost? A boolean `retryable` cannot say that, because whether
a retry is safe depends on the operation too.

## Decision

- `BrokerException(Status, ErrorInfo(reason, domain, metadata), Dispatch, Optional<NodeId>)`.
- `Status` is the gRPC canonical code set, including `OUT_OF_RANGE` and `DATA_LOSS`.
- `ErrorInfo` follows AIP-193: a stable reason (`CREDENTIALS_REJECTED`, `RESOURCE_GONE`,
  `HAS_CONSUMERS`, `DUPLICATE_REJECTED`, `DESTINATION_FULL`, `INVALID_FILTER`, …) and native codes
  in `metadata`. Sensitive option values never enter it.
- `Dispatch` is `NOT_SENT`, `IN_DOUBT` or `REJECTED`. A commit without an answer is `IN_DOUBT`.
- A cluster-wide call returns `Partial<T>`, naming each node's outcome (`APPLIED`, `FAILED`,
  `IN_DOUBT`, `SKIPPED`) and failure. A partly applied write is never rolled back by the SDK.

## Consequences

- Callers can retry exactly what is safe and report what is unknown.
- Drivers must classify every failure; the TCK checks the standard reasons.

## Alternatives considered

- **Exception subclasses per failure.** A class hierarchy cannot express status and dispatch together.
- **`retryable` flag.** Wrong for non-idempotent operations whose request may have left.
