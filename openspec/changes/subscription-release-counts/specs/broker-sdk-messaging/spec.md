## MODIFIED Requirements

### Requirement: Capabilities are declared per subscription kind
A transport SHALL declare, for each subscription kind it offers, the orderings, the pull settlement scope, whether transactions can be joined, what the system enforces for retries, and whether a release still counts as a delivery attempt. Asking for the capabilities of a kind the transport does not offer SHALL fail with `INVALID_ARGUMENT`. A declaration that depends on the cluster's features SHALL be computed at connect and SHALL be re-checked when a processor opens.

#### Scenario: Two subscription kinds on one system
- **WHEN** a consumer reads the capabilities of a transport that offers an ordered subscription kind and an unordered queue kind
- **THEN** each kind reports its own orderings, settlement scope, joined transactions and retry enforcement

#### Scenario: A release on a system that counts every redelivery
- **WHEN** a message is released without a failed delivery on a kind that declares that a release counts
- **THEN** the redelivered message's delivery count is higher than before

#### Scenario: A release on a system that counts only failures
- **WHEN** a message is released without a failed delivery on a kind that declares that a release does not count
- **THEN** the redelivered message's delivery count is not higher than before
