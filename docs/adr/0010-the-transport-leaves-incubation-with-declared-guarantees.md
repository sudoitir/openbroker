# ADR-0010: The transport leaves incubation with declared guarantees

- **Status**: accepted; takes the transport out of the incubating types named in [ADR-0008](0008-semver-0x-with-japicmp-and-incubating-types.md)
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

The incubating transport carried messages but stated no guarantee: a caller could not tell whether a
send survived a broker restart, or whether a failed receive was redelivered. Systems differ here, and
Kafka, JetStream, AMQP 1.0 and JMS each name the guarantee on the producer, consumer or session, not
on the message. A client that accepts a guarantee it cannot keep loses data silently.

## Decision

- `Guarantee {AT_MOST_ONCE, AT_LEAST_ONCE, TRANSACTED}` is set on `SenderOptions` and
  `ReceiverOptions`, one value per link.
- `Transport.capabilities()` returns a static `TransportCapabilities`. Opening a link outside it
  fails with `UNIMPLEMENTED` / `NOT_SUPPORTED`, request not sent. A driver keeps what it accepted
  through recovery ([ADR-0011](0011-drivers-own-recovery-and-failover.md)).
- `AT_LEAST_ONCE` and `TRANSACTED` sends are durable. A message without a `durable` header is sent
  durable; an explicit `durable=false` is refused with `INVALID_ARGUMENT`.
- "Effectively once" is `AT_LEAST_ONCE` plus `idempotent`, as Kafka and JetStream state it.
- `Capability.TRANSPORT_TRANSACTED` is removed from `access`: it is a static fact of the driver, not
  an account permission.
- `@Incubating` leaves the transport; the next release is 0.2.0, a breaking minor.

## Consequences

- Every sender and receiver says what it promises, and a driver that cannot keep it says so before
  any message moves.
- Every transport type changes, plus `SendReceipt`, `ConnectOptions` and `Capability`; adopters of the
  incubating transport must port. Nothing is kept for them.
- The TCK gets a case per guarantee a driver declares.

## Alternatives considered

- **Kafka-style `acks` numbers.** Shaped by one system.
- **A guarantee per message.** A link's session mode is fixed in every system.
- **An `EXACTLY_ONCE` value.** No system gives it end to end, so the name would lie.
- **Honouring non-durable sends under at-least-once.** The guarantee would silently not survive a
  broker restart.
