# ADR-0003: `BrokerDrivers` is the entry point, and drivers are found by `ServiceLoader`

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

Adopters run in plain Java, in DI containers and in hosts that load third-party code. They need one
place to connect, shared transports, their own credentials, TLS, deadlines and call policy, and no
chance that untrusted code contributes a driver.

## Decision

- `BrokerDrivers` is an `AutoCloseable` instance. `load()` uses `ServiceLoader` with the SDK's own class
  loader, never the thread context class loader; `of(List)` is for containers. It owns transports
  shared per TLS material and closes them.
- `connect(ConnectRequest(system, ClusterSpec, CredentialSource, ConnectOptions))` returns a
  thread-safe `ClusterConnection` with facets: nodes, resources, operations, messages, access,
  notifications and the transport. `reconfigure(ClusterSpec)` follows topology changes without a
  reconnect. Credentials and TLS are asked for on every (re)connect, so rotation needs no restart.
- A `CallInterceptor` admits each management and control call (`CallInfo`: node, endpoint, plane,
  operation, cost, target) and is told the outcome. Per-message sends and receives are not intercepted.
- Deadlines travel in `ScopedValue<Deadline> CallScope.DEADLINE`; drivers carry it onto their threads
  and raise `DEADLINE_EXCEEDED`. `contextPropagation` wraps every task a driver hands to another thread.
- The SPI (`.spi`) is separate from the API; consumers never need it. Calls block and suit virtual
  threads; receivers, senders and subscriptions are single-owner.

## Consequences

- Hosts can apply rate limits, metrics and tracing without the SDK knowing their framework.
- A blocking API; the messaging client's change adds an async one.

## Alternatives considered

- **A static registry.** Hard to close and to isolate in tests and containers.
- **Thread context class loader discovery.** Lets loaded plugin code inject a driver and see credentials.
