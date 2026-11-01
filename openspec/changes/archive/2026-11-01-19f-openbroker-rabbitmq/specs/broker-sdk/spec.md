## ADDED Requirements

### Requirement: An etag states what it can tell apart
A `Preview` SHALL carry an `EtagBasis`: `INCARNATION` when its etag holds a system-assigned identity, so that an
operation on a resource recreated since the preview is refused, or `CONFIGURATION` when the system exposes no such
identity and a resource recreated with the same configuration cannot be told apart. The TCK SHALL check the
recreate refusal only for `INCARNATION`.

#### Scenario: A system without incarnation ids
- **WHEN** a driver previews an operation on a system that exposes no incarnation id
- **THEN** the preview's basis is `CONFIGURATION`, and the TCK does not require the recreate refusal

### Requirement: A message the system routed nowhere has a reason
The SDK SHALL define `ErrorReasons.NOT_ROUTED`, and a driver SHALL report it whenever the system says it accepted
a message that no subscription received.

#### Scenario: An unroutable send
- **WHEN** the system reports a sent message as unroutable
- **THEN** the send fails with status `FAILED_PRECONDITION` and reason `NOT_ROUTED`

### Requirement: Message actions are SDK capabilities
The SDK SHALL define `Capability.MOVE_MESSAGES`, `COPY_MESSAGES` and `DELETE_MESSAGES`, so every driver declares
them by the same ids.

#### Scenario: Two drivers declare deleting messages
- **WHEN** two drivers declare message deletion
- **THEN** both use `Capability.DELETE_MESSAGES`

### Requirement: The TCK skips what a kind does not offer
A TCK case that opens a browsing receiver SHALL require the kind's `BROWSE` verb, and one that opens a consuming
receiver SHALL require `RECEIVE`, so a driver that honestly lacks a verb is skipped, not failed.

#### Scenario: A kind without browse
- **WHEN** the TCK runs against a kind that does not declare `BROWSE`
- **THEN** the browsing cases are skipped with the reason
