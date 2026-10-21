# ADR-0013: Push processing runs in lanes on virtual threads

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

The pull `Receiver` settles per message on most systems but, on Artemis, a release rolls back every
unsettled message of the session. Callers also want concurrency without hand-built thread pools, and
ordering by group, partition or subscription as Artemis groups, JetStream and Service Bus sessions
offer. JMS listeners are single-threaded; a callback on a Netty thread blocks I/O.

## Decision

- `Transport.process(subscription, scope, ProcessorOptions, MessageHandler)` returns a `Processor`.
  `concurrency` (1 to 64) is the number of lanes. A lane is a receiver holding at most one unsettled
  message, run on a virtual thread, so settlement is per message on every system.
- The handler returns a `Disposition`: accept, release (optionally delayed, counted as a failure) or
  reject. A thrown exception is a counted release.
- `OrderingScope`: `NONE`; `GROUP` (one `groupId` runs serially, in order); `PARTITION`;
  `SUBSCRIPTION` (total order, concurrency 1).
- The engine lives in `openbroker-support`. A driver supplies lanes and declares whether the system
  pins groups to a lane; otherwise the engine hashes `groupId` or key to a lane.
- Receive credit is `FlowControl(maxUnsettled, prefetchMessages, prefetchBytes)`.
  `capabilities().prefetchBounds()` says which bound the system enforces: BYTES on Artemis, MESSAGES
  on AMQP and RabbitMQ, both on Kafka.
- The pull `Receiver` stays; `capabilities().pullSettlement()` is `RECEIVER` where a release covers
  the whole receiver.

## Consequences

- Handlers are plain blocking code; Java 25 (JEP 491) does not pin on `synchronized`, and OpenBroker
  code uses `ReentrantLock` and never runs user code on a Netty thread. A TCK case requires no
  `jdk.VirtualThreadPinned` event.
- Each lane costs one session, so concurrency times nodes sessions.
- Memory is bounded by the prefetch bound plus the handed-out messages.

## Alternatives considered

- **A callback on I/O threads.** Blocks the client.
- **`CompletionStage`-returning handlers.** Unneeded with virtual threads, and two handler shapes
  would split every retry path.
- **JMS single-threaded `MessageListener`.** No concurrency within a session.
- **One `prefetch` field.** It is meaningless on byte-window systems.
