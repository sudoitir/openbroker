# ADR-0017: Chaos is part of the TCK

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

The recovery contract ([ADR-0011](0011-drivers-own-recovery-and-failover.md)) is only as good as
its tests, and a broker that is never cut, slowed or failed over proves nothing about it. The TCK
runs against a real broker and its driver must keep what it declares (CLAUDE.md, non-negotiable 7).

## Decision

- `openbroker-tck` gains `ChaosTck`. Toxiproxy sits in front of each broker's data port and gives
  cut, latency and timeout; a stable proxy port also survives stopping and starting the broker.
- Failover is tested with a shared-store primary and backup pair on a shared volume.
- Each guarantee has a case for every column of the loss/duplicate table.
- Chaos runs in its own CI job, on the oldest and newest supported broker.

## Consequences

- The contract in the README is executable.
- The job is slower and can be flaky; it does not block the fast build.
- The shared-store setup must work on the oldest broker image too.

## Alternatives considered

- **Replication HA.** Needs a cluster connection and quorum; slower and flakier to bring up.
- **An embedded in-JVM broker.** It would not test the network.
