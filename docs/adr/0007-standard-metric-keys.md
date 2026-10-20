# ADR-0007: Standard metric keys

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

Every broker reports load under its own names (messageCount, lag, messages_ready). Dashboards and
alerts across systems need one set, with honest units and resets.

## Decision

Metrics are standard attributes under `openbroker.` keys in the OpenTelemetry style with UCUM units:
`openbroker.backlog`, `openbroker.backlog.size`, `openbroker.backlog.age`, `openbroker.in_flight`,
`openbroker.scheduled`, `openbroker.enqueued`, `openbroker.acknowledged`, `openbroker.expired`,
`openbroker.delivered`, `openbroker.sent`, and counts of consumers, subscriptions, producers and
channels. A `MetricValue` carries its precision, the start instant of a cumulative counter (so a reset
or restart never fakes a rate) and a breakdown per placement. A metric a system cannot report is
absent or unavailable, never zero.

## Consequences

- One alert rule works on every system that reports the key.
- A driver maps its native counters once; the TCK checks them.

## Alternatives considered

- **Native names passed through.** Every consumer maps them again.
