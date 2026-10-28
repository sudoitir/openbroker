## Context
`Disposition.Release(delay, deliveryFailed)` says whether the delivery failed. What a system does with an uncounted release differs: Kafka share groups count every redelivery, consumer groups and Artemis count only failed ones.

## Decision
A plain `boolean releaseCountsDelivery` on `SubscriptionCapabilities`, the last component. It is a fact of the subscription kind like `joinedTransactions`, has two honest values on every system, and an enum would add no case. The TCK reads the redelivered message's `deliveryCount` after an uncounted release: it must rise where declared true and must not where declared false. No ADR: the shape follows ADR-0021's per-kind capabilities.

## Risks
Adding a record component breaks callers that construct the record (japicmp), hence `0.6.0`.
