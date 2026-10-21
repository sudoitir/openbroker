# ADR-0016: Performance budgets are enforced in CI

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

Per-message allocation and throughput decide whether a relay can keep up, and they regress silently.
Wall-clock numbers on shared CI runners are noisy; allocation per operation is deterministic.

## Decision

- `openbroker-benchmarks` is a JMH module that is not published: codec, send, process, round-trip and
  observer benchmarks, the broker ones against an in-VM Artemis server.
- A budgets runner runs them with the GC profiler and compares the result with a checked-in
  `budgets.json`, exiting non-zero on a breach.
- Allocation per operation is a hard gate at the baseline plus the larger of 10% and 64 B.
  Throughput has a floor at half the baseline median and p99 latency a ceiling at twice the baseline.
- Baselines come from a green CI run and are committed. Raising a budget needs a commit that says why.
- `MessagingObserver.none()` must add zero bytes.

## Consequences

- An allocation regression fails the build.
- Throughput and latency gates catch collapses, not drift.
- Baselines are tied to `ubuntu-latest` and move with it.

## Alternatives considered

- **Throughput gates only.** Too noisy on shared runners to be hard gates.
- **Benchmarks run by hand.** Regressions arrive unseen.
