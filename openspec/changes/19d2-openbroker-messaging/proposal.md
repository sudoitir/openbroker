## How to run this change
This change states **requirements only**. Run it in a fresh session:

1. `git fetch`, then create a git worktree on a new branch off `origin/main`, and remove it after the merge.
2. Read this proposal, its specs, the `broker-sdk` and `broker-sdk-artemis` capabilities and the ADRs they cite.
3. Brainstorm and investigate. Check the client libraries with ctx7, and study the prior art: the Kafka producer and consumer, the AMQP 1.0 and JMS 3.1 clients, the Pulsar client, the NATS JetStream client, Spring Cloud Stream, MassTransit, NServiceBus, Dapr pub/sub and the OpenTelemetry messaging conventions. Ask only what is really the maintainer's to decide.
4. `/opsx:update`: add `design.md`, sharpen the specs and replace the stub `tasks.md`.
5. `/opsx:apply` with the harness under **Execution**. New decisions get an ADR.
6. Verify: the TCK, including the chaos cases, against every driver that declares the capabilities; the JMH budgets met.
7. PR, merge on green CI, `/opsx:archive`, and release a new minor version.

## Why
OpenBroker is meant for critical backends, such as payments and enterprise integration, that need sending and receiving to be easy, secure, reliable and fast on every broker. Today's `Transport` carries only what a management tool's relay needs, and it is marked `@Incubating`. A production messaging client has to state its guarantees exactly and keep them through failures. Its shape must settle before more drivers implement it, so each driver implements it once.

## What Changes
- **Delivery guarantees, declared per sender and receiver**: at most once, at least once, and transacted (exactly once within one system). A driver declares which it can honour.
- **Idempotent send**, using the broker's deduplication where it has one (Artemis duplicate detection, Kafka idempotent producers, SQS FIFO deduplication) and declared unavailable where it has none.
- **Ordering per key and per group**, with the ordering scope stated: the partition, the message group, or none.
- **Credit-based flow control and backpressure** on receive, and bounded buffering on send. Running out is a typed outcome, never an unbounded queue.
- **Consumer concurrency**, with dispatch that is ordered per key or unordered, and the in-flight limit as a setting.
- **Reconnect and failover across HA nodes**, with a stated contract for loss and duplicates for each guarantee, and connection-state listeners.
- **Asynchronous APIs** next to the blocking ones, designed for virtual threads without pinning.
- **Retry and dead-letter policies**: redelivery delay, backoff and maximum attempts, mapped to the broker's own mechanism where one exists.
- **Security**: mTLS, credential rotation without dropping the receivers in flight, and per-destination authorization errors.
- **Observability**: OpenTelemetry messaging spans and metrics through hooks, with trace context carried in message headers. `openbroker-api` stays dependency-free.
- **Performance**: JMH benchmarks with budgets for throughput, latency and allocations per message, run in CI.
- **TCK**: a case for every guarantee, including chaos (a broker killed mid-flight, a network partition, a failover).
- `@Incubating` comes off `Transport`, and the transport types are versioned like the rest of the API. **BREAKING** for adopters of the incubating types.

## Capabilities
### New Capabilities
- `broker-sdk-messaging`: The production messaging client: guarantees, idempotence, ordering, flow control, concurrency, failover, async, retry, security, observability, performance and its TCK.
### Modified Capabilities
- `broker-sdk`: `Transport` leaves incubation.
- `broker-sdk-artemis`: the Artemis driver implements the messaging client.

## Out of scope
- Schema registries and payload serialization formats.
- Stream processing (joins, windows, state stores).
- Drivers for further systems; each brings the messaging client with it.

## Depends on
- 19c-broker-sdk: the SDK, the message model and the Artemis driver.

## Execution
**Subagent-driven**, planned in its own session.
