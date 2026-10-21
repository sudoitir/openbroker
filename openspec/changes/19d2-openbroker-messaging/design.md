# Design: 19d2-openbroker-messaging

## Context
`Transport` (`@Incubating`) carries what a management tool's relay needs: pull receivers, single-thread senders, transacted batches. It has no stated guarantee, no async send, no backpressure, no failover, no retry policy and no telemetry. On Artemis, Core links connect once with `reconnectAttempts=0`, and a release or reject settles every unsettled message of the receiver.

This change makes `Transport` a production messaging client and takes it out of incubation (0.2.0). Prior art studied:
- the Kafka producer and consumer (idempotence, `acks`, `max.in.flight`, order per partition, `buffer.memory` / `max.block.ms`);
- AMQP 1.0 and JMS 3.1 (link credit, outcomes, a session-scoped transaction);
- Pulsar (`sendAsync`, `blockIfQueueFull`, negative acknowledgement with a redelivery delay, DLQ policy per consumer);
- NATS JetStream (`AckPolicy`, `MaxAckPending`, `Nats-Msg-Id` with a duplicate window, `NakWithDelay`, `MaxDeliver`);
- Spring Cloud Stream and Spring Kafka (blocking retries keep order, retry topics break it);
- MassTransit and NServiceBus (immediate retries in process, delayed redelivery by scheduled copy, an error queue);
- Dapr pub/sub (a retry resiliency policy, a dead-letter topic);
- the OTel messaging conventions.

Artemis facts, checked with ctx7:
- **Duplicate detection:** `_AMQ_DUPL_ID`; `id-cache-size` is 20000 per address by default; `persist-id-cache` is true by default. A duplicate in a transaction rejects the whole transaction; a non-transacted duplicate is dropped silently.
- **Send acknowledgements:** async acknowledgements need `confirmationWindowSize > 0` and a `SendAcknowledgementHandler`. `blockOnDurableSend` defaults to true and `blockOnNonDurableSend` to false.
- **Window sizes:** `producerWindowSize` is a byte credit, and a send blocks while the address is full under BLOCK. `consumerWindowSize` is in bytes: 0 means no buffer and -1 means unbounded.
- **Failover:** a transacted session becomes rollback-only and its commit throws `TRANSACTION_ROLLED_BACK`. A commit that is unblocked by failover has an unknown outcome. The docs recommend duplicate ids for exactly once.
- **Redelivery and dead-lettering:** `redelivery-delay`, `redelivery-delay-multiplier`, `max-redelivery-delay`, `max-delivery-attempts` (default 10) and `dead-letter-address` are all per address.

## Goals
- Declare every guarantee on each sender and receiver; a driver refuses what it cannot keep with `UNIMPLEMENTED`, and keeps what it accepted through recovery.
- Idempotent send, ordering scopes, credit-based receive, a bounded send buffer, concurrent processing, async alongside blocking.
- Driver-owned recovery and failover, with a loss/duplicate contract for each guarantee, and link-state events.
- Retry and dead-letter policy; mTLS; credential rotation; per-destination authorization errors.
- OTel through a hook; JMH budgets in CI; a TCK case per guarantee, including chaos.

## Non-goals
- XA or distributed transactions.
- Exactly once across two systems. A relay keeps two commits plus duplicate ids.
- Schema registries, stream processing, new drivers.
- The Spring Boot starter. It is a follow-up; every options type is a plain record with `defaults()` and `withX` methods that a `@ConfigurationProperties` class maps one to one.

## Decisions

**D1. Guarantees are an enum on each link, and refusal is static.**
- `Guarantee {AT_MOST_ONCE, AT_LEAST_ONCE, TRANSACTED}` goes on `SenderOptions` and `ReceiverOptions`. `Transport.capabilities()` returns a static `TransportCapabilities`, and opening a link outside it fails with `UNIMPLEMENTED`/`NOT_SUPPORTED`, `NOT_SENT`.
- `Capability.TRANSPORT_TRANSACTED` is removed from `access`: it is a static fact, not an account permission.
- "Effectively once" is not a guarantee value. It is `AT_LEAST_ONCE` plus `idempotent`, which is how Kafka and JetStream state it.
- Rejected:
  - Kafka-style `acks` numbers: system-shaped.
  - A per-message guarantee: a link's session mode is fixed in every system.
  - An `EXACTLY_ONCE` value: no system gives it end to end, so the name would lie.

**D2. `AT_LEAST_ONCE` and `TRANSACTED` sends are durable.**
- A message without a `durable` header is sent durable. An explicit `durable=false` is refused with `INVALID_ARGUMENT`.
- Rejected: honouring non-durable under at-least-once. The guarantee would silently not survive a broker restart.

**D3. Bounded send pipeline: `send` blocks, `offer` never does.**
- `SendBuffer(maxInFlight, maxBytes, maxBlock)` bounds messages that are accepted but not yet confirmed.
- `offer()` returns `Offer.Accepted(CompletionStage<SendReceipt>)` or `Offer.Full(inFlight, bytes)`; it never blocks and never throws for a full buffer.
- `send()` waits for room for up to `maxBlock`, then fails with `RESOURCE_EXHAUSTED`/`SEND_BUFFER_FULL`, `NOT_SENT`. Otherwise it waits for the guarantee's confirmation.
- Non-transacted senders are thread-safe (one writer per link, as in Kafka). Transacted ones have a single owner.
- Rejected:
  - Kafka's `send()`, which blocks on `max.block.ms` and has no non-blocking form.
  - Pulsar's `blockIfQueueFull=false`, which throws: an exception per message on the hot path.
  - An unbounded queue.
  - A reactive-streams `Publisher`: a dependency, and with virtual threads `send` already composes.

**D4. Receive credit is a count; prefetch is bounded in the unit the system can enforce.**
- `FlowControl(maxUnsettled, prefetchMessages, prefetchBytes)`. The receiver stops reading once `maxUnsettled` messages are handed out and unsettled.
- The client buffers ahead at most `prefetchMessages` and/or `prefetchBytes`, whichever the system enforces; `capabilities().prefetchBounds()` says which. Artemis enforces BYTES, AMQP and RabbitMQ enforce MESSAGES, Kafka enforces both.
- With both prefetch values at 0, nothing is buffered. Memory is bounded by the prefetch bound plus `maxUnsettled` handed-out messages, and large bodies are spooled to disk as today.
- Rejected: a single "prefetch" field. That was the old `OptionalInt`, and it is meaningless on byte-window systems.

**D5. Push processing on virtual threads, in lanes.**
- `Transport.process(subscription, scope, ProcessorOptions, MessageHandler)` returns a `Processor`. `concurrency` is the number of lanes. A lane is a receiver holding at most one unsettled message, run on a virtual thread, so settlement is per message on every system.
- `ordering`:
  - `NONE`: any lane takes any message.
  - `GROUP`: messages with one `groupId` run serially, in order.
  - `PARTITION`: per partition.
  - `SUBSCRIPTION`: total order, so concurrency must be 1.
- The engine lives in `openbroker-support` (rule: fix the gap in the SDK, not in one driver). A driver supplies lanes and declares whether the system pins groups to a lane, as Artemis groups, JetStream or Service Bus sessions do. Otherwise the engine hashes `groupId`/`key` to a lane.
- The pull `Receiver` stays. On a system whose release covers the whole receiver, its `capabilities().pullSettlement()` is `RECEIVER`. Artemis is such a system for a release without delay.
- Rejected:
  - A callback on Netty threads (blocks I/O).
  - `CompletionStage`-returning handlers: there is no need with virtual threads, and two handler shapes would split every retry path.
  - JMS `MessageListener` single-threaded sessions.

**D6. Drivers own recovery and failover; the broker client's own failover is not used.**
- `Recovery(Backoff, giveUpAfter)`. On link loss the link goes to `RECOVERING` and reopens on its node. If that node has an HA group (`Membership.group` from the `Nodes` facet, with peer Core endpoints taken from the `ClusterSpec`), it reopens on the group's serving node.
- It then becomes `OPEN` with `servedBy`, or `FAILED` after `giveUpAfter`. Every reopen attempt is a data-plane control call admitted by the `CallInterceptor` (operation `link.recover`). `LinkListener` events are serial per link.
- Rejected: Artemis's `ha=true` locator with `reconnectAttempts` and session re-attach.
  - It dials topology-announced connectors, which are unreachable behind NAT, proxies or Toxiproxy. That is the reason `useTopologyForLoadBalancing=false` exists today.
  - Re-attach hides the failure from the contract: a non-transacted send can be lost silently.
  - The `name=` connector matching is not on the oldest supported broker (2.33).
  - Kafka, Pulsar and NATS clients all own their reconnect; the SDK's contract has to be the same on every driver.

**D7. Loss and duplicates are a stated contract (table below).**
- After recovery:
  - Unconfirmed `AT_LEAST_ONCE` sends are resent in their original order, and the receipt is marked `resent=true`.
  - `AT_MOST_ONCE` sends are never resent: their receipts fail `UNAVAILABLE`, `IN_DOUBT`.
  - Settling a message received on a link that has since recovered fails with `FAILED_PRECONDITION`/`DELIVERY_STALE`, `NOT_SENT`, because the message will be delivered again.
  - A transacted link's commit after recovery fails with `ABORTED`/`TRANSACTION_ROLLED_BACK`, `REJECTED`.
- Order is kept across a resend only when the sender is idempotent. `ordering != NONE` with `AT_LEAST_ONCE` therefore requires `idempotent=true`; otherwise `INVALID_ARGUMENT`.

**D8. Idempotence is the system's deduplication, keyed by the existing `deduplicationId` header.**
- With `idempotent=true`, a message without an id gets a UUIDv7, so the driver's own resends are deduplicated.
- Application retries reuse their own id.
- `Sender.deduplication()` reports the effective `DeduplicationWindow` per destination: a number of ids (Artemis `id-cache-size`), a time (JetStream, SQS FIFO, Service Bus) and whether it survives a restart.
- A system without id deduplication (Kafka) declares `deduplication=false`. Kafka's idempotent producer only covers its own retries, which a driver may use internally.
- Rejected: a client-side dedupe cache. It is not shared across producers or restarts, so it would be false comfort.

**D9. Retry and dead-letter policy with an honest split between system and client.**
- `RetryPolicy(mode, maxAttempts, backoff, deadLetter)`.
- `SYSTEM` (the default): a handler failure releases the message counted, and the system's own policy applies (Artemis address settings). Nothing in the client.
- `MANAGED`: the client counts attempts. Then:
  - **ordered processing** retries in place: the lane holds the message, waits out the backoff and calls the handler again. This keeps order, as Spring Kafka's blocking retries do.
  - **unordered processing** settles `Release(delay = backoff(n), deliveryFailed=true)` and does not hold the lane.
  - **after `maxAttempts`** the message is rejected to `deadLetter`, or to the system's dead-letter destination.
- `capabilities().retry()` says, for delay, attempts and dead-letter separately, whether the system or the client enforces each.
- An explicit `Reject` dead-letters now, without retries.
- Rejected:
  - Retries only in process (MassTransit immediate): long delays starve lanes.
  - Retries only by copy (NServiceBus delayed): breaks group order.
  - Always the broker's policy: Artemis cannot set it per consumer.

**D10. A transaction can span a receive and its sends.**
- `Receiver.sender(target)` and `MessageContext.sender(target)` return a sender joined to the receiver's transaction on the same node: Artemis uses the same Core session, Kafka would use `sendOffsetsToTransaction`, JMS the same session.
- This is the "exactly once within one system" path: consume, transform, produce, commit once.
- Rejected: two commits plus duplicate ids as the only way. It stays valid across systems, but within one system it is weaker and costs more.

**D11. Telemetry is a hook in the API, implemented in an optional module.**
- `ConnectOptions.observer(MessagingObserver)`; the default is `MessagingObserver.none()`, which allocates nothing.
- The driver, or the `-support` engine, calls `onSend`, `onReceive`, `onProcess` and `onSettle` with an `OperationInfo`. On send the observer injects carrier entries, which become string properties (`traceparent`, `tracestate`).
- `openbroker-opentelemetry` depends only on `openbroker-api` and `io.opentelemetry:opentelemetry-api`. It defines the attribute names as constants, because the semconv messaging attributes are still in development.
- Rejected:
  - A Micrometer Observation API: a second facade.
  - The OTel API in `openbroker-api`: breaks non-negotiable 1.
  - Java-agent instrumentation only: it does not know OpenBroker's links.

**D12. Virtual threads without pinning.**
- Java 25 includes JEP 491 (JDK 24): `synchronized` and `Object.wait` no longer pin, so the Artemis client's monitors are fine.
- OpenBroker code uses `ReentrantLock`, never blocks inside `ConcurrentHashMap.compute`, and never runs user code or completes a future on a Netty thread. Completions hop to the driver's virtual-thread executor, wrapped by `contextPropagation`.
- A TCK case records the JFR event `jdk.VirtualThreadPinned` with threshold 0 and requires none.

**D13. Security.**
- **mTLS** comes from `TlsMaterial.context()` with key managers. It already reaches Core through `CoreTls`.
- **Rotation:** `CredentialSource` and `TlsProvider` are asked again at every link open and recovery. Open links are never torn down for rotation. A replaced TLS material's Core factory is reference-counted and closed with its last session, which fixes today's leak.
- **Authorization:** a refusal on one destination is `PERMISSION_DENIED`/`NOT_AUTHORIZED`, `REJECTED`, with metadata `openbroker.plane=DATA` and `openbroker.resource=<id>`. On async sends it arrives on the receipt, through `SendAcknowledgementHandler.sendFailed`. The link stays `OPEN`.

**D14. Performance budgets.** `openbroker-benchmarks` is a JMH module that is not published. A budgets runner gates allocation per message hard; throughput and latency get wide floors and ceilings.

**D15. Chaos TCK.** Toxiproxy sits in front of each broker's Core port and gives cut, latency and timeout. A stable proxy port also survives a `docker kill` and `docker start` of the broker. A shared-store Artemis primary/backup pair, on a shared Docker volume, is used for failover. These run in a separate CI job.
- Rejected:
  - Replication HA: needs a cluster connection and quorum, and is slower and flakier to bring up.
  - An in-JVM embedded broker: it would not test the network.

## Public API sketch (`io.github.sudoitir.openbroker.messaging`; `@Incubating` removed everywhere)
```java
public enum Guarantee { AT_MOST_ONCE, AT_LEAST_ONCE, TRANSACTED }
public enum OrderingScope { NONE, GROUP, PARTITION, SUBSCRIPTION }
public enum LinkState { OPEN, RECOVERING, FAILED, CLOSED }
public enum PrefetchBound { MESSAGES, BYTES }
public enum SettlementScope { MESSAGE, RECEIVER }
public enum Enforcement { SYSTEM, CLIENT }
public enum RetryMode { SYSTEM, MANAGED }
public enum ReceiveMode { CONSUME, BROWSE }                       // unchanged

public record Backoff(Duration initial, double multiplier, Duration max, double jitter) {
    public static Backoff defaults() { /* 100 ms, 2.0, 10 s, 0.2 */ }
    public Duration delay(int attempt) { ... }                    // attempt >= 1, capped at max
}
public record Recovery(Backoff backoff, Optional<Duration> giveUpAfter) {
    public static Recovery defaults() { /* Backoff.defaults(), never give up */ }
}
public record SendBuffer(int maxInFlight, long maxBytes, Duration maxBlock) {
    public static SendBuffer defaults() { /* 1000, 16 MiB, 30 s */ }   // all > 0, validated
}
public record FlowControl(int maxUnsettled, int prefetchMessages, long prefetchBytes) {
    public static FlowControl defaults() { /* 100, 100, 1 MiB */ }
    public static FlowControl none() { /* 1, 0, 0 */ }             // maxUnsettled >= 1, prefetch >= 0
}
public record RetryPolicy(RetryMode mode, int maxAttempts, Backoff backoff, Optional<SendTarget> deadLetter) {
    public static RetryPolicy system() { ... }
    public static RetryPolicy managed(int maxAttempts, Backoff backoff) { ... }
    public RetryPolicy withDeadLetter(SendTarget target) { ... }
}
public record SenderOptions(Guarantee guarantee, boolean idempotent, OrderingScope ordering,
                            SendBuffer buffer, Recovery recovery) {
    public static SenderOptions defaults() { /* AT_LEAST_ONCE, false, NONE, defaults, defaults */ }
    public SenderOptions withGuarantee(Guarantee g); withIdempotent(boolean b); withOrdering(OrderingScope o);
    public SenderOptions withBuffer(SendBuffer b); withRecovery(Recovery r);
}
public record ReceiverOptions(ReceiveMode mode, Guarantee guarantee, Optional<MessageFilter> filter,
                              Optional<Instant> producedAtOrBefore, FlowControl flow, Recovery recovery) {
    public static ReceiverOptions consume() { /* CONSUME, AT_LEAST_ONCE, ..., FlowControl.defaults() */ }
    public static ReceiverOptions browse(Optional<MessageFilter> filter) { ... }
    public ReceiverOptions withGuarantee(Guarantee g); withFlow(FlowControl f); withRecovery(Recovery r);
    public ReceiverOptions producedAtOrBefore(Instant bound);
}                                                                  // replaces ReceiveOptions
public record ProcessorOptions(ReceiverOptions receive, int concurrency, OrderingScope ordering,
                               RetryPolicy retry, Duration stopGrace) {
    public static ProcessorOptions defaults() { /* consume(), 8, NONE, system(), 30 s */ }
    public ProcessorOptions withConcurrency(int n); withOrdering(OrderingScope o); withRetry(RetryPolicy r);
}
public record DeduplicationWindow(OptionalLong ids, Optional<Duration> time, boolean durable) {}
public record RetrySupport(Enforcement delay, Enforcement attempts, Enforcement deadLetter) {}
public record TransportCapabilities(
        Set<Guarantee> guarantees, boolean deduplication, Set<OrderingScope> orderings,
        Set<PrefetchBound> prefetchBounds, SettlementScope pullSettlement,
        boolean joinedTransactions, RetrySupport retry, boolean failover) {}

public record SendReceipt(Optional<String> messageId, Optional<MessageRef> ref,
                          Optional<NodeId> node, boolean resent) {}
public sealed interface Offer {
    record Accepted(CompletionStage<SendReceipt> receipt) implements Offer {}
    record Full(int inFlight, long bufferedBytes) implements Offer {}
}
public record LinkEvent(String link, NodeId node, LinkState state, Optional<NodeId> servedBy,
                        Optional<BrokerException> cause, Instant at) {}
@FunctionalInterface public interface LinkListener { void changed(LinkEvent event); }

public interface Transport {
    TransportCapabilities capabilities();
    Sender sender(SendTarget target, Scope scope, SenderOptions options);
    Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options);
    Processor process(ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler);
    AutoCloseable watch(LinkListener listener);
}
public interface Sender extends AutoCloseable {      // thread-safe unless TRANSACTED
    String id();
    LinkState state();
    Optional<DeduplicationWindow> deduplication();
    SendReceipt send(OutboundMessage message);        // waits for room (maxBlock), then for confirmation
    Offer offer(OutboundMessage message);             // never blocks
    void flush(Duration timeout);                     // every accepted message confirmed, or DEADLINE_EXCEEDED
    void commit();                                    // TRANSACTED only; no answer => IN_DOUBT
    void rollback();
    @Override void close();                           // flush(maxBlock), then fails the rest CANCELLED
}
public interface Receiver extends AutoCloseable {    // single owner
    String id();
    LinkState state();
    List<Message> receive(int max, Duration timeout); // stops at maxUnsettled; empty while RECOVERING
    void settle(Message message, Disposition disposition);
    Sender sender(SendTarget target);                 // joined to this TRANSACTED receiver's transaction
    void commit();
    void rollback();
    @Override void close();
}
public interface Processor extends AutoCloseable {
    String id();
    LinkState state();
    void stop(Duration grace);       // stop taking; wait for handlers; release the rest uncounted
    @Override void close();          // stop(options.stopGrace())
}
@FunctionalInterface public interface MessageHandler { Disposition handle(MessageContext context) throws Exception; }
public interface MessageContext {
    Message message();
    int attempt();                   // 1-based: system delivery count, plus in-place retries
    Sender sender(SendTarget target); // TRANSACTED processors only: joined to this delivery's transaction
}
// Disposition: unchanged shape (Accept | Release(delay, deliveryFailed) | Reject(reason)); @Incubating removed.

// Telemetry hook
public enum OperationType { SEND, RECEIVE, PROCESS, SETTLE }
public record OperationInfo(String messagingSystem, OperationType type, String operation,
        ResourceId destination, Optional<ResourceId> subscription, NodeId node,
        Optional<String> serverAddress, OptionalInt serverPort, int messageCount) {}
public interface Observation {
    default AutoCloseable scope() { return () -> {}; }   // makes the span current around a handler
    default void received(List<Message> messages) {}     // pull: add links
    void end(@Nullable Throwable error);                 // receipt completion, handler end, settle end
}
public interface MessagingObserver {
    Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier);
    Observation onReceive(OperationInfo op);
    Observation onProcess(OperationInfo op, Message message);
    Observation onSettle(OperationInfo op, Message message, Disposition disposition);
    static MessagingObserver none() { ... }               // a shared no-op; allocates nothing
}
```

Changes in the core package `io.github.sudoitir.openbroker`:
- `ConnectOptions` gains the field `MessagingObserver observer`, plus `withObserver`.
- `ClusterConnection.transport()` loses `@Incubating`.
- `ErrorReasons` gains `SEND_BUFFER_FULL`, `DELIVERY_STALE`, `TRANSACTION_ROLLED_BACK`, `NOT_SUPPORTED`, and the metadata key `RESOURCE = "openbroker.resource"`.
- `Capability.TRANSPORT_TRANSACTED` is removed.

SPI: no new types. Drivers implement `Transport` and build on `openbroker-support`'s `io.github.sudoitir.openbroker.support.messaging`:
- `SendPipeline`: bounded window, one writer virtual thread, confirmation tracking, resend on recovery.
- `ProcessorEngine`: lanes, ordering, retries, observer calls.
- `LinkSupervisor`: states, backoff, events, interceptor admission.
- `Backoffs`.

## Artemis mapping

| Feature | Broker does | Client (driver or `-support`) does |
|---|---|---|
| Link | one Core session per sender, receiver lane or joined pair | `CoreLinks` factory per endpoint, TLS id and settings; `useTopologyForLoadBalancing=false`, `reconnectAttempts=0`; connect outside `compute` (per-key lock) |
| AT_MOST_ONCE send | — | `blockOnDurableSend=false`, `blockOnNonDurableSend=false`; receipt completes on write; never resent |
| AT_LEAST_ONCE send | persists, then acknowledges (`journal-sync-non-transactional`) | `confirmationWindowSize=1 MiB`; `producer.send(addr, msg, SendAcknowledgementHandler)`; receipt completes on `sendAcknowledged`; `sendFailed` → mapped error; forces durable |
| TRANSACTED send | transacted session; duplicate in tx → whole tx refused | receipts complete at `commit`; `ActiveMQTransactionRolledBackException` → `ABORTED`/`TRANSACTION_ROLLED_BACK`; outcome unknown → `IN_DOUBT` |
| Send backpressure | `producerWindowSize` byte credits; address BLOCK or FAIL | `SendPipeline` window (`maxInFlight`, `maxBytes`); writer is a virtual thread, so credit waits block only it; FAIL → `RESOURCE_EXHAUSTED`/`DESTINATION_FULL` |
| Idempotence | `_AMQ_DUPL_ID`, `id-cache-size` per address, `persist-id-cache` | sets `_AMQ_DUPL_ID` from `deduplicationId`, or a UUIDv7 when `idempotent`; window read once per destination via Jolokia `getAddressSettingsAsJSON` (`idCacheSize`), else 20000; `durable` from `persist-id-cache` (assumed true when unreadable) |
| Ordering GROUP | `_AMQ_GROUP_ID` pins a group to one consumer (lane) | lanes are separate consumers; GROUP across more than one node → `INVALID_ARGUMENT` |
| Ordering SUBSCRIPTION | FIFO queue | concurrency must be 1; total order across processes needs an exclusive queue (documented) |
| Ordering PARTITION | none | `UNIMPLEMENTED` |
| Credit / prefetch | `consumerWindowSize` bytes per consumer | `consumerWindowSize = prefetchBytes / lanes` (0 → no buffer); stops reading at `maxUnsettled`; `prefetchBounds={BYTES}` |
| AT_MOST_ONCE receive | `preAcknowledge=true` session | `settle(Accept)` no-op; Release/Reject → `INVALID_ARGUMENT` |
| AT_LEAST_ONCE receive | — | `individualAcknowledge`, `blockOnAcknowledge=false` (a lost ack = a duplicate, allowed) |
| Release, no delay | rollback; `redelivery-delay` and its multiplier apply when counted | `session.rollback(counted)`: covers every unsettled message of the receiver, hence `pullSettlement=RECEIVER`; a lane holds one |
| Release with delay | scheduled delivery (`_AMQ_SCHED_DELIVERY`) | sends a copy to the queue's FQQN (`address::queue`) with the delay, `openbroker.attempt=n+1`, `_AMQ_DUPL_ID` removed, headers kept; confirmed, then acks the original (crash in between = duplicate, never loss) |
| Reject / dead-letter | `dead-letter-address` (address settings) | sends a copy to `retry.deadLetter` or the address's DLA (Jolokia), with `_AMQ_ORIG_ADDRESS`, `_AMQ_ORIG_QUEUE`, `_AMQ_ORIG_MESSAGE_ID` and `openbroker.dead_letter_reason`; confirmed, then acks; no DLA → ack (drop), as Artemis does |
| Retry SYSTEM | `max-delivery-attempts`, redelivery delay, DLA | Release counted; nothing else |
| Retry MANAGED | — | attempts = max(Core delivery count, `openbroker.attempt`) + in-place retries; open refused `FAILED_PRECONDITION` with a broker.xml remedy when `maxAttempts` exceeds the address's `max-delivery-attempts` (not -1); `retry()=CLIENT/CLIENT/CLIENT` |
| Joined transaction | one Core transacted session | producer on the receiver's session; target on the same node |
| Recovery / failover | shared store or replication: the backup holds the journal and the dedupe cache | `LinkSupervisor` reopens on the node, then on the serving member of its HA group (`Membership.group`, Core endpoints from `ClusterSpec`); recreates consumers and producers; resends; `clientFailureCheckPeriod` and `connectionTTL` from `Timeouts` |
| Credentials / TLS | authenticates at session create; existing sessions keep running | asks again on every open and recovery; ref-counted factories per TLS id |
| Authorization | `ActiveMQSecurityException` at create, send or `sendFailed` | `PERMISSION_DENIED`/`NOT_AUTHORIZED`, `REJECTED`, metadata plane and resource |

## Loss/duplicate contract (as messages reach the destination or the handler)

| Guarantee | Link lost or partition | Broker crash and restart (no HA) | Failover to HA backup | Client process dies | Confirmation or commit answer lost |
|---|---|---|---|---|---|
| AT_MOST_ONCE send | unconfirmed may be lost; never resent; receipts `UNAVAILABLE`/`IN_DOUBT` | non-durable lost | unconfirmed may be lost | buffered lost | n/a (no confirmation) |
| AT_MOST_ONCE receive | prefetched and handed-out lost | same | same | same | n/a |
| AT_LEAST_ONCE send | resent in order: no loss, duplicates possible (`resent=true`) | confirmed survive (durable); unconfirmed resent | same | unconfirmed in buffer lost; their receipts never completed | resent: duplicate possible |
| AT_LEAST_ONCE + idempotent send | resent; deduplicated: no loss, no duplicate, order kept | same, while within the dedupe window | same (dedupe cache persisted or replicated) | as above | resent and deduplicated |
| AT_LEAST_ONCE receive / process | unsettled redelivered (after `connectionTTL` on a half-open link); stale settle → `DELIVERY_STALE` | redelivered | redelivered | redelivered | lost ack → redelivered (duplicate) |
| TRANSACTED (incl. joined) | transaction rolled back; commit → `ABORTED`; redo | rolled back | rolled back (rollback-only) | rolled back | `IN_DOUBT`; redo with the same dedupe ids → exactly once |

Every case ends `FAILED` after `giveUpAfter`: pending receipts fail `UNAVAILABLE`, `IN_DOUBT`, and nothing is resent afterwards.

## OTel mapping (`openbroker-opentelemetry`, `OpenTelemetryMessaging.of(OpenTelemetry)`)

| Hook | Span name | Kind | Key attributes | Links / context |
|---|---|---|---|---|
| `onSend` (send, offer) | `send {destination}` | PRODUCER (its context is the creation context) | `messaging.system` (driver gives it; Artemis `activemq`), `messaging.operation.type=send`, `messaging.operation.name=send`, `messaging.destination.name`, `messaging.message.id` (on receipt), `messaging.message.conversation_id`, `messaging.message.body.size`, `server.address`, `server.port`, `error.type` (= reason) | injects `traceparent` and `tracestate` through the configured `TextMapPropagator`; span ends at receipt completion |
| `onReceive` (pull) | `receive {subscription}` | CLIENT | `messaging.operation.type=receive`, `messaging.destination.subscription.name`, `messaging.batch.message_count` | a link to each message's extracted creation context |
| `onProcess` | `process {subscription}` | CONSUMER | `type=process`, `messaging.message.id` | link to the creation context; `scope()` makes it current around the handler |
| `onSettle` | `settle {subscription}` | CLIENT | `type=settle`, `messaging.operation.name` = accept, release or reject | for `Receiver.settle` and `commit`, and processor Release/Reject; processor Accept is not a span |

Metrics:
- `messaging.client.operation.duration` (histogram, s): send, receive, settle.
- `messaging.client.sent.messages` (counter, with `error.type`).
- `messaging.client.consumed.messages` (counter).
- `messaging.process.duration` (histogram, s, with the semconv bucket advice).
- `openbroker.messaging.link.recoveries` (counter, with `openbroker.link.state`).

## JMH benchmarks and budgets (`openbroker-benchmarks`, not published)

Benchmarks:
- `CoreCodecBench`: `CoreBodies.write` and `read` of a 1 KiB message (no broker).
- `SendBench`: `offer` plus confirmation, and `send`, against an embedded in-VM Artemis server (`artemis-server`, non-persistent, test-only dependency), for AT_MOST_ONCE and AT_LEAST_ONCE+idempotent.
- `ProcessBench`: Processor, concurrency 8, AT_LEAST_ONCE.
- `RoundTripBench` (`SampleTime`): send to handler, p99.
- `ObserverBench`: `none()` against a counting observer. Zero added bytes for `none()`.

Measurement: `Runner` with `GCProfiler`, 1 fork, 3×2 s warmup, 5×2 s measurement, about 3 minutes. The `Budgets` main reads `RunResult` and compares it with the checked-in `budgets.json`, then exits non-zero on a breach.
- **Allocation** (`gc.alloc.rate.norm`, B/op) is a hard gate at baseline + max(10%, 64 B). It is deterministic.
- **Throughput floor** is 50% of the baseline median on `ubuntu-latest`, and the **p99 ceiling** is 2× the baseline. Shared runners are noisy, so these catch collapses, not drift.

The baselines come from the first green CI run of this PR and are committed in it. Raising a budget needs a commit that says why.

## TCK
In `openbroker-tck`:
- New abstract `MessagingTck`, using `TckEnvironment`.
- New abstract `ChaosTck`, using `TckChaos`: `ConnectRequest request()`, `haRequest()`, `NodeId node()`, `primary()`, `backup()`, `void cut(NodeId)`, `heal(NodeId)`, `kill(NodeId)`, `start(NodeId)`.
- `TckEnvironment` gains `Optional<ConnectRequest> restrictedRequest()` (an account that may receive from `tck.*` but not send to `tck.denied.*`) and `Optional<ConnectRequest> mutualTlsRequest()`.
- A case is skipped when the driver declares the feature unavailable.

`MessagingTck` cases:
1. `atMostOnceNeverRedelivers`
2. `atLeastOnceRedeliversUnsettledAfterClose`
3. `transactedRollbackRedeliversAndCommitRemoves`
4. `joinedTransactionMovesExactlyOnce`
5. `undeclaredGuaranteeOrOrderingIsUnimplemented`
6. `explicitNonDurableUnderAtLeastOnceIsRefused`
7. `sameDeduplicationIdIsStoredOnce`, and the same across two senders
8. `deduplicationWindowIsReported`
9. `groupOrderKeptWithConcurrency` (3 groups × 200, concurrency 4)
10. `subscriptionOrderKept`
11. `receiverNeverHoldsMoreThanCredit` (`maxUnsettled=5`, prefetch 0; a second receiver takes the rest)
12. `processorNeverRunsMoreThanConcurrency`
13. `rejectDeadLettersAtOnce`
14. `releaseWithDelayIsHiddenUntilDue`
15. `managedRetryBacksOffThenDeadLetters` (gaps ≥ backoff, reason property)
16. `orderedManagedRetryKeepsOrder`
17. `systemRetryReleasesCounted`
18. `unauthorizedDestinationIsPermissionDeniedWithResource` (for both send and receive)
19. `mutualTlsConnects`
20. `observerLinksProcessToSend` (a fake observer; checks `traceparent` on the received message)
21. `processingDoesNotPinVirtualThreads` (JFR `jdk.VirtualThreadPinned`, threshold 0)
22. `listedDestinationReportsDeliveryModes` (the regression guard for the delivery-mode bug)
23. The existing transport cases, ported.

`ChaosTck` cases:
1. `brokerKilledMidSendIdempotentIsExactlyOnce`
2. `brokerKilledMidSendAtLeastOnceLosesNothing` (every confirmed id is present; every duplicate has a `resent` receipt)
3. `brokerKilledAtMostOnceNeverDuplicates`
4. `brokerKilledMidProcessLosesNothing`
5. `commitDuringKillIsInDoubtOrAbortedAndRedoIsExactlyOnce`
6. `partitionRecoversWithEvents` (RECOVERING, then OPEN, in order)
7. `partitionFillsSendBufferThenFull` (exactly `maxInFlight` accepted, then confirmed after heal)
8. `staleSettleAfterRecoveryIsDeliveryStale`
9. `failoverAtLeastOnceLosesNothing` (the spec scenario)
10. `failoverIdempotentSendIsExactlyOnce`
11. `failoverEventNamesServingBackup`
12. `credentialRotationKeepsReceiversAndRecoversWithNewCredentials`

Tooling:
- Testcontainers `Network`.
- `ToxiproxyContainer` (`testcontainers-toxiproxy`, BOM-managed; image pinned in `toxiproxy.image`), set up following that module's 2.0.x docs, with one proxy per broker on its Core port. The Jolokia port is mapped directly.
- Kill and start through the Docker client (`killContainerCmd` / `startContainerCmd`): the proxy port stays stable.
- An HA pair of Artemis containers with `ha-policy` `shared-store` primary/backup, through a broker.xml override (`etc-override`) and a shared named volume for the journal, `failover-on-shutdown=true`.
- Chaos timeouts: `Timeouts.fixed(1s, 1s)`, so detection is about 1–3 s.

Two ITs in `openbroker-artemis`:
- `ArtemisMessagingTckIT`: in `verify`, on both images.
- `ArtemisChaosIT`: failsafe profile `chaos`, as its own CI job on both images in parallel with `verify`, with a 15-minute timeout.

## Module and dependency changes
- `openbroker-api`: the messaging types above; no new dependency.
- `openbroker-support`: the `support.messaging` package.
- `openbroker-tck`: `MessagingTck`, `ChaosTck`, `TckChaos`; `testcontainers` is not added, because the TCK stays broker-agnostic.
- **New** `openbroker-opentelemetry`: depends on `openbroker-api` and `io.opentelemetry:opentelemetry-api` (managed by `opentelemetry-bom`); tests use `opentelemetry-sdk-testing`. Published, in the BOM, covered by japicmp from the next release (no previous version yet; skip with `japicmp.skip` until 0.2.0 exists).
- **New** `openbroker-benchmarks`: depends on `jmh-core`, `jmh-generator-annprocess`, the Artemis driver, `artemis-server` (`${artemis.version}`) and `openbroker-opentelemetry`. Not deployed, not in the BOM, skipped by japicmp and Sonar coverage.
- `openbroker-artemis` (test scope): `testcontainers-toxiproxy`.
- Root: version `0.2.0-SNAPSHOT` in every pom; `openbroker.previous.version` stays `0.1.0`.
- CI: a `chaos` job (matrix of both images) and a `bench` job (newest image not needed, because the broker is embedded). Both are required in `ci-ok`.

## Risks and trade-offs
- **Resend without idempotence duplicates.** That is stated in the contract, and the `resent` flag tells callers which receipts were resent.
- **Dedupe window.** Artemis's id window is a count per address. A burst bigger than `id-cache-size` between the original and its resend can duplicate. The window is reported, and documented.
- **Client-side dead-letter and delayed copies change the message id at the broker level.** The headers are kept, plus the Artemis `_AMQ_ORIG_*` properties, so tools still trace them. A crash between the confirmed copy and the ack gives a duplicate, never a loss.
- **Half-open links.** A message held by a dead session is redelivered only after the broker's `connectionTTL`. That latency is documented, and the TTL is driven by `Timeouts`.
- **Managed retries can exceed the broker's limits.** The open is refused when they are readable. When management is unreachable, the effective limit is min(policy, broker): a documented caveat.
- **Lanes cost one session each.** Concurrency × nodes sessions. The 64-lane cap is validated.
- **HA groups depend on management.** Without a readable `Membership`, recovery only reopens the same node.
- **The shared-store container setup** (`etc-override`, file locks on a named volume) must work on the 2.33 image too. Task 6.10 verifies it first, and falls back to a mounted `broker.xml` if needed.
- **JMH throughput gates on shared runners** are coarse by design. The allocation gate is the precise one.
- **Breaking change for adopters.** Every transport type changes, plus `SendReceipt`, `ConnectOptions` and `Capability`.

## ADRs (Nygard, `docs/adr/`)
- **0010 The transport leaves incubation with declared guarantees.** Links declare at-most-once, at-least-once or transacted; static `TransportCapabilities` refuse the rest with `UNIMPLEMENTED`.
- **0011 Drivers own recovery and failover.** Links reopen through the SDK's supervisor onto the HA group's serving node, under a loss/duplicate contract per guarantee; native client failover is not used.
- **0012 Sending is bounded and backpressure is a value.** A bounded in-flight window; `offer` returns `Full`, `send` waits `maxBlock`; at-least-once resends, ordered only when idempotent.
- **0013 Push processing runs in lanes on virtual threads.** Per-message settlement through one-message lanes, with GROUP, PARTITION or SUBSCRIPTION ordering; the engine lives in `-support`.
- **0014 Retries default to the system's policy; managed retries run in the client.** In place when ordered, delayed redelivery when unordered, dead-letter after max attempts; each aspect declares whether the system or the client enforces it.
- **0015 Telemetry goes through a hook, with OpenTelemetry in an optional module.** `MessagingObserver` in the API; `openbroker-opentelemetry` maps it to semconv spans and metrics and W3C context in properties.
- **0016 Performance budgets are enforced in CI.** JMH with a hard gate on allocations per message, and wide throughput and latency floors.
- **0017 Chaos is part of the TCK.** Toxiproxy and a shared-store HA pair, in a separate CI job on both broker versions.

