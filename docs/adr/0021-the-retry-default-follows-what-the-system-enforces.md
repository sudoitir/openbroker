# ADR-0021: The retry default follows what the system enforces, per subscription kind

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi
- **Supersedes**: the default and the `RetrySupport` declaration in [ADR-0014](0014-retries-default-to-the-systems-policy-managed-retries-run-in-the-client.md),
  which stays accepted for the rest of its decision

## Context

[ADR-0014](0014-retries-default-to-the-systems-policy-managed-retries-run-in-the-client.md) made
`RetryMode.SYSTEM` the default and declared who enforces each part of a policy as one
`RetrySupport(Enforcement delay, attempts, deadLetter)` per transport. Two things were wrong.

- **A default that some systems must refuse.** A Kafka consumer group has no system retry, so a
  default processor would be refused there. The defaults are meant to be usable on every system.
- **A declaration that said little.** Artemis declared `CLIENT` for all three parts although the
  broker enforces a `SYSTEM` retry there. In practice the value only said that under `MANAGED` the
  client does the work, which is true on every system. Its one reader, the TCK, misread it as whether
  a dead-letter destination may be named.

Kinds of one system also differ. A share group and a consumer group are different groups on the
broker, with different orderings, settlement and retry, so one declaration per transport is dishonest
for the kind it does not describe.

## Decision

- `SystemRetry(delays, countsAttempts, deadLetters)`, with `SystemRetry.NONE`, says what the system
  itself enforces under `SYSTEM`. `MANAGED` is always available, in the client. `RetrySupport` and
  `Enforcement` are removed.
- `SubscriptionCapabilities(orderings, pullSettlement, joinedTransactions, retry)` holds what differs
  by kind. `TransportCapabilities` keeps a map of them, read with `subscription(KindId)`, which throws
  `IllegalArgumentException` for a kind the transport does not offer. The map is static, with no I/O.
  Its `orderings` now apply to senders.
- `RetryMode.DEFAULT` is the mode of `RetryPolicy.defaults()`, which `ProcessorOptions.defaults()`
  uses. `RetryPolicy.system()` stays as an explicit choice.
- `Retries.resolve(RetryPolicy, SystemRetry)` in `openbroker-support` resolves it, and every driver
  calls it when it opens a processor, before its own checks.
  - `DEFAULT` becomes `SYSTEM` where `countsAttempts`, and otherwise `MANAGED` with the policy's
    attempts (10 for the defaults, the delivery limit common brokers dead-letter at) and backoff.
    A driver retries that managed default in place, so a default processor creates no retry
    destinations.
  - An explicit `SYSTEM` where nothing is counted fails `UNIMPLEMENTED`, request not sent.
  - `MANAGED` is kept and may always name a dead-letter target.
- `Processor.retry()` returns the resolved policy, so a consumer can see what the default became.
- Artemis declares `SystemRetry(true, true, true)` for its queue kind.

## Consequences

- The defaults work on every system, and the choice is visible in `Processor.retry()`.
- A driver says per kind what its system does, and the TCK runs its defaults case for each kind it can
  make, with a case for the refused explicit `SYSTEM`.
- Retry and dead-letter destinations a driver needs for an explicit, unordered `MANAGED` policy are
  that driver's decision to record.
- This is a breaking change: code that relied on the `SYSTEM` default reads `Processor.retry()`, and
  code that read `capabilities.retry()`, `pullSettlement()` or `joinedTransactions()` reads
  `capabilities.subscription(kind)`.

## Alternatives considered

- **Emulate a system retry in the client.** Hides where enforcement happens, which ADR-0014 made explicit.
- **Keep `SYSTEM` as the default and declare a retry for every system.** The default would still be
  refused where nothing is counted.
- **A lookup by `ResourceId` or a method on `Transport`.** A static map has no I/O, and every
  `ResourceId` already carries its kind.
