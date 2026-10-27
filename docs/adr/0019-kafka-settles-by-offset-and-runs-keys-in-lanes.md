# ADR-0019: Kafka settles by offset and runs keys in lanes

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

A Kafka consumer does not settle messages. It reads a partition in offset order and commits one
number per partition: everything before it is done. The SDK settles each message on its own, from
many lanes at once ([ADR-0013](0013-push-processing-runs-in-lanes-on-virtual-threads.md)), in any
order. Committing the offset of the message that settled last would lose every message before it that
is still running; committing nothing until a batch ends would stall a slow message's partition.

A consumer must also keep polling to stay in its group, even when its handlers are slow, and a
rebalance interrupts its poll with a callback that has a deadline.

## Decision

- **One poll thread per consumer, handlers elsewhere.** A platform thread polls; handlers run on
  virtual threads in the engine's lanes. No consumer call is made from a handler thread. Settlement
  is thread-safe state that the poll thread reads when it commits.
- **A tracker per partition holds every offset from fetch to settlement.** The commit position is the
  lowest offset not yet settled, or one past the highest offset fetched when all are settled. A record
  that is fetched but waiting, queued, paused or running is never committed past. Gaps from
  compaction and transaction markers do not block it.
- **Commits are asynchronous on a cadence and synchronous on revoke and close.** After a revoke,
  records settled beyond the first unsettled one may be delivered again. That is inside the duplicate
  contract.
- **Lanes are routed by the ordering asked for.** `PARTITION` hashes a partition to a lane; `GROUP`
  hashes the group (or the key) within the partition; keyless records are their own group; `NONE` takes
  the shortest queue. `SUBSCRIPTION` runs one lane and is allowed only on a one-partition topic,
  checked at open. Each lane runs one record at a time, so a lane's records stay in order, and the
  system pins groups to lanes: the engine adds no locks.
- **Credit is the unsettled bound, enforced by pausing.** When as many records as the credit are
  handed out and unsettled, the consumer pauses its partitions and keeps polling. Records already
  fetched wait in the consumer and are handed out as credit returns, so the bound is exact, not a hint.
- **A release hands the record out again from the same consumer.** The record stays unsettled, so the
  commit position waits behind it; nothing is sought. A delay holds it back until due.
- **A revoke drains, then commits.** The partition stops being handed out, queued records are taken
  back, the handlers already running on it finish within the grace time, what they settled is committed
  synchronously, and the rest is handed back uncounted. The callback runs on the poll thread, so the
  grace must be shorter than the group's rebalance timeout (`max.poll.interval.ms`): a longer one is
  refused at open with `INVALID_ARGUMENT`, naming both.
- **A lost partition is dropped at once.** Nothing is committed, and settling any of its records fails
  `DELIVERY_STALE`.
- **At most once commits before the record is handed out; at least once commits after settlement.**
  A transacted processor commits a lane's offsets inside the lane's transaction, with the consumer's
  group metadata (KIP-447), so a member the group has dropped cannot commit. Each lane has a producer
  with a `transactional.id` fixed for the processor's life (the processor's id and the lane's index),
  because a producer holds one transaction at a time and lanes run at once. The commit position inside
  a transaction is computed as if the lane's records had settled, so another lane's unsettled record
  still holds it back.
- **A record with a future due time is not taken.** The partition is sought back to it and paused until
  it is due (the retry topics of [ADR-0021](0021-the-retry-default-follows-what-the-system-enforces.md)).

## Consequences

- A slow message holds back its partition's commit position, and so redelivery on a crash or rebalance,
  but never other partitions, and never the other lanes' progress.
- Memory held per partition is bounded by the credit.
- The poll thread does the commits, so a commit is as slow as one poll interval at worst.
- A transacted processor with many lanes has many producers.
- A consumer that is paused for credit still heartbeats and still hears a rebalance.

## Alternatives considered

- **Confluent Parallel Consumer.** It owns its threads, commit cadence and retry, which clash with the
  lanes and the retry policies, and its maintenance pace is slower than the clients'. Its two ideas,
  contiguous commits and per-key queues, are small and are written here against the TCK.
- **Commit the offset of the last settled record.** It loses records that were still running when the
  process stopped.
- **Seek back to release a record.** It re-reads everything fetched after the record, duplicating
  records that other lanes hold, and races the fetch buffer.
- **One producer for all lanes of a processor.** A producer has one open transaction, so lanes would
  take turns and the processor would run one record at a time.
- **A `transactional.id` per set of partitions.** The set changes at every rebalance, so ids would
  churn and fence nothing.
