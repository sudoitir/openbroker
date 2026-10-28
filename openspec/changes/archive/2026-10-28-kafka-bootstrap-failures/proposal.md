## Why
A consumer connecting to a Kafka cluster found that an address nothing can reach is reported badly. A bootstrap server whose name does not resolve fails as an internal error ("Failed to create new KafkaAdminClient"). A server on a closed port is retried by the client until the whole deadline and reported as a timeout, while the client logs a rebootstrap line about every millisecond.

## What Changes
- Before a client is made for the bootstrap servers, each is resolved and connected to. The client is given only the servers that answer; when none does, the call fails at once as unavailable and not sent, naming the cause of the first server (`kafka.reach_failure`: `dns`, `refused` or `timeout`, and `kafka.endpoint`) and listing every server in the message.
- A client that cannot be built because no bootstrap server resolves is reported the same way.
- Clients wait longer between attempts to reconnect to a broker, so a broker that goes away costs a bounded, quiet number of attempts.
- The TCK checks that a driver classifies an address nothing answers on, unresolvable or refusing connections.
- A new kind, `kafka.hanging-transaction`: transactions the coordinator no longer tracks, left open on a partition and holding its last stable offset back (KIP-664). They are listed on demand, scoped to the cluster or one topic and bounded, and RESOLVE aborts one with a preview and an etag.
- No API change.

## Capabilities
### New Capabilities
None.

### Modified Capabilities
- `broker-sdk-kafka`: unreachable bootstrap servers are classified before a client is made; hanging transactions are found and aborted.

## Impact
`openbroker-kafka` (`Bootstrap`, `Sockets`, `KafkaCluster`, `KafkaErrors`, `Reach`), `openbroker-tck` (`OpenBrokerTck`, `TckEnvironment.requestToAddress`), the Artemis TCK environment. Released as `0.6.2`.
