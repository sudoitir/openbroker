## Why
RabbitMQ is the most widely run AMQP broker, and its routing model differs from both Artemis and Kafka: virtual
hosts, exchanges that route to queues by binding, classic, quorum and stream queues, shovels and federation. A
RabbitMQ driver brings OpenBroker's single vocabulary to RabbitMQ estates, and it is the SDK's third test before
its stable release: what it cannot express is fixed in the SDK, for every driver.

## What Changes
- A new driver module, `openbroker-rabbitmq` (system id `rabbitmq`), managing a cluster through the management HTTP
  API and moving messages over AMQP 0-9-1, registered through `ServiceLoader`, supported from RabbitMQ 3.13.
- Virtual hosts are namespaces; exchanges are destinations; classic, quorum and stream queues are subscriptions
  (one kind each, since their verbs differ); bindings, exchange-to-exchange bindings, shovels and federation links
  are routes; policies and permissions are policies and access rules; quorum members are leader and follower
  placements.
- Browse only on streams, where it never consumes; classic and quorum queues declare browse unavailable and
  fetch nothing. Settlement by acknowledgement; publisher confirms with `mandatory`.
- Events from the event exchange where it is enabled and permitted, and a polling watch where not.
- **BREAKING (SDK)**: `Preview` gains a `basis` (`EtagBasis.INCARNATION` or `CONFIGURATION`): a system that exposes
  no incarnation id cannot promise that a recreated resource is refused, and now says so. The TCK checks that
  refusal only for `INCARNATION`.
- SDK additions: `Capability.MOVE_MESSAGES`, `COPY_MESSAGES`, `DELETE_MESSAGES` (moved from the Kafka driver);
  `ErrorReasons.NOT_ROUTED`; three TCK cases now check the kind's verbs before opening a receiver.

## Capabilities
### New Capabilities
- `broker-sdk-rabbitmq`: the RabbitMQ driver.
### Modified Capabilities
- `broker-sdk`: the etag basis, the message-action capabilities, `NOT_ROUTED`, and the TCK's verb checks.

## Out of scope
- Creating shovels, federation upstreams and users: they carry secrets the SDK cannot yet accept write-only, so
  those kinds are read and delete only until it can.
- The stream protocol client (browse on streams uses AMQP 0-9-1 with `x-stream-offset`), super streams, and
  RabbitMQ benchmarks.

## Impact
A new module, the BOM, the TCK, `Preview` call sites in the Kafka and Artemis drivers, the CI matrix
(`rabbitmq:3.13.7-management`, `rabbitmq:4.4.0-management`), coverage wiring, README.
