## Why
A consumer reading a large cluster (thousands of partitions, hundreds of groups) found per-item rounds in the Kafka driver: a page of subscriptions asked for the partitions of each topic one by one, and a read of several queries repeated the group listing, the group descriptions and the offsets for every query. The cost grew with the size of the page instead of staying constant.

## What Changes
- A page of subscriptions asks for the partitions of all its topics in one round.
- A read of several queries is one call to the cluster in which the queries share their rounds: the group listing, the groups' descriptions and offsets, the topics' partitions and their log offsets are each asked once for everything the queries name. A query that fails still fails alone.
- No API change.

## Capabilities
### New Capabilities
None.

### Modified Capabilities
- `broker-sdk-kafka`: reads are batched per page and per call.

## Out of scope
- Previewing or running a reset for all of a group's topics at once: the SDK's operations act on one subscription, so each costs its own rounds.

## Impact
`openbroker-kafka` (`KafkaResources`, `Subscriptions`, `GroupInfo`, `Offsets`, a new `Reads`). Released as `0.6.1`.
