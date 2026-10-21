# ADR-0014: Retries default to the system's policy; managed retries run in the client

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

Systems retry differently. Artemis sets redelivery delay, attempts and dead-letter address per
address, not per consumer. Pulsar has a negative acknowledgement with a delay and a DLQ policy per
consumer; JetStream has `NakWithDelay` and `MaxDeliver`. Spring Kafka's blocking retries keep order,
while retry topics break it. MassTransit retries in process first and redelivers by scheduled copy; Dapr
puts retries in a resiliency policy.

## Decision

- `RetryPolicy(mode, maxAttempts, backoff, deadLetter)`.
- `SYSTEM` (the default): a handler failure releases the message counted, and the system's policy
  applies. The client does nothing more.
- `MANAGED`: the client counts attempts.
  - Ordered processing retries in place: the lane holds the message, waits out the backoff and calls
    the handler again, which keeps order.
  - Unordered processing releases with `delay = backoff(n)` and does not hold the lane.
  - After `maxAttempts` the message is rejected to `deadLetter`, or to the system's dead-letter
    destination.
- An explicit `Reject` dead-letters at once.
- `capabilities().retry()` is a `RetrySupport` that says, for delay, attempts and dead-letter
  separately, whether the system or the client enforces each.
- A managed policy that exceeds a limit the system enforces and the driver can read is refused at open
  with `FAILED_PRECONDITION`, naming the remedy.

## Consequences

- The default does what the operator configured on the broker; managed retries are opt-in.
- Client-side delayed and dead-letter copies get a new broker message id. Headers are kept, with the
  system's origin properties, so tools still trace them. A crash between the confirmed copy and the
  acknowledgement duplicates; it never loses.
- When management is unreachable the effective limit is the smaller of the policy and the broker's.

## Alternatives considered

- **Retries only in process.** Long delays starve lanes.
- **Retries only by scheduled copy.** Breaks group order.
- **Always the broker's policy.** Artemis cannot set it per consumer.
