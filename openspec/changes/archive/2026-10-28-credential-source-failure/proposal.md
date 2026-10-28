## Why
A driver asks the consumer's `CredentialSource` on its own threads, on every connect and reconnect. The drivers retry
on a `BrokerException`, but a source that fails any other way, such as a lookup in the consumer's own store that fails
or is interrupted, ends that thread with an uncaught exception. A notification stream or a consumer link that does
this never reconnects.

## What Changes
- `BrokerDrivers` hands every driver the consumer's source behind a guard. A failure of the source that is not a
  `BrokerException` reaches the driver as `UNAVAILABLE` (`UNREACHABLE`, not sent). The message names the plane and the
  failure's type, never its text. A `BrokerException` from the source passes through unchanged.
- No API change.

## Impact
- `openbroker-api`: `BrokerDrivers.connect` and `verify`.
- Every driver: a failing source now backs off and retries like any unavailable broker.
