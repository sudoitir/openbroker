## Purpose

Defines OpenBroker's consumer API and driver SPI: one vocabulary for managing, observing and moving messages on every message broker and queue service, with the guarantees every driver keeps and proves against the TCK.

## Requirements

### Requirement: The SDK is a framework-free library with a driver SPI
The SDK SHALL ship as `openbroker-api` (the consumer API and the driver SPI in separate packages, with a `module-info.java`), `openbroker-support`, `openbroker-tck`, `openbroker-bom` and `openbroker-parent`, targeting Java 25, from its own public repository.
- `openbroker-api` SHALL depend on nothing except nullness annotations.
- No driver, `openbroker-support` or `openbroker-tck` SHALL depend on a framework; Spring SHALL appear only in the optional Spring Boot modules.
- The SDK SHALL carry no product type or brand other than its own.

#### Scenario: An outside project adopts the SDK
- **WHEN** a project with no Spring dependency adds `openbroker-api` and a driver
- **THEN** it can connect to a cluster through `BrokerDrivers` and use every facet the driver offers, without importing the SPI

#### Scenario: Spring leaks into a driver
- **WHEN** a change adds a Spring dependency to a driver or to `openbroker-api`
- **THEN** the build fails its dependency check

### Requirement: Consumers connect through one entry point
A consumer SHALL connect through `BrokerDrivers` in the API package, never through the SPI. Drivers SHALL be found through `ServiceLoader` on the SDK's own class loader, never the thread context class loader, or be supplied explicitly. Connecting and verifying SHALL refuse a cluster whose options break the driver's connection schema (an unknown key, a missing required key, a wrong type or a value that is not allowed), before any call is sent, naming every key and never a sensitive value.

#### Scenario: A driver is discovered
- **WHEN** a driver jar is on the class or module path of the SDK's class loader
- **THEN** `BrokerDrivers.load()` finds it, and the driver names its system, supported versions, connection schema and native packages

#### Scenario: A plugin cannot contribute a driver
- **WHEN** code running in a child class loader carries a driver service file and calls `BrokerDrivers.load()`
- **THEN** that driver is not loaded and receives no credentials

#### Scenario: A misspelt option
- **WHEN** a consumer connects with a cluster option its driver's schema does not declare
- **THEN** the connect fails with an invalid-argument error, not sent, naming the option

#### Scenario: A sensitive option has a wrong value
- **WHEN** a sensitive option's value is not of its declared type
- **THEN** the error names the key and never contains the value

### Requirement: A connection follows the cluster without reconnecting
A `ClusterConnection` SHALL accept a changed cluster specification and SHALL apply it without closing the resources of nodes that did not change. Credentials and TLS material SHALL be asked for on every (re)connect, so rotation needs no reconnect. A cluster without addressable nodes SHALL be reported as one synthetic node.

#### Scenario: A node joins
- **WHEN** a cluster specification gains a node while receivers and notification subscriptions are open on the others
- **THEN** the open receivers and subscriptions continue, and the new node is usable

#### Scenario: A managed queue service
- **WHEN** a driver connects to a service that exposes no nodes
- **THEN** it reports one synthetic node, and partial results are keyed by it

### Requirement: Resources have an open kind over a closed resource class
Every resource SHALL be identified by a `ResourceId` of a driver-defined kind and a segmented path, with one canonical, percent-encoded text form.
- Parsing SHALL reject a non-canonical encoding, so equal text means an equal id.
- Every kind SHALL map to one resource class: namespace, destination, partition, subscription, consumer group, route, policy, access rule, principal, connection, channel, producer, consumer or transaction.
- A subscription's destination relation SHALL be optional.

#### Scenario: Shared code reasons about classes
- **WHEN** a consumer lists the subscriptions of two clusters of different systems
- **THEN** it gets resources of both systems' native kinds, each of class `SUBSCRIPTION` with the standard attribute keys

#### Scenario: A canonical name round-trips
- **WHEN** a resource id whose path segments contain `/` or `%` is formatted and parsed, and its text is parsed and formatted
- **THEN** both round trips give back the original

#### Scenario: A non-canonical name is refused
- **WHEN** a text id encodes a character that needs no encoding, such as `%41`
- **THEN** parsing fails

#### Scenario: A queue with no destination
- **WHEN** a driver reports a standalone queue
- **THEN** it is a `SUBSCRIPTION` with no destination relation, and messages are sent to it as a subscription

### Requirement: Kinds declare their capabilities and attributes
Every kind SHALL declare its verbs, its supported orderings and an attribute schema that marks each attribute as config, status or metric, with an instrument and unit for metrics. Every kind SHALL also carry its system's display label, singular and plural, in sentence case, so that a consumer never derives a label from the kind id.

#### Scenario: An ordering the kind does not support
- **WHEN** a consumer asks for a list ordered by a field the kind does not declare
- **THEN** the call fails with `INVALID_ARGUMENT`

#### Scenario: A kind names itself
- **WHEN** a consumer lists the kinds of any driver
- **THEN** every kind has a non-blank singular and plural label in the system's own words, and the TCK fails a driver whose kind has none

### Requirement: Health and membership are expressed in neutral terms
Resources and nodes SHALL carry conditions (type, true/false/unknown, reason, message, last transition). A node SHALL state its configured designation (primary, backup or none) separately from whether it is serving now, and its functions (broker, controller, storage).

#### Scenario: A blocked destination
- **WHEN** a destination stops accepting messages because it is full
- **THEN** its resource carries a condition that says so, with the native reason

#### Scenario: A backup has taken over
- **WHEN** the backup of an HA pair becomes live
- **THEN** it is reported as designated backup and serving active

### Requirement: Work is scoped to shards
Node, message and operation calls SHALL take a scope: the whole resource, or a set of shards, where a shard is a node instance or a partition. Placements SHALL name the node, the optional partition, the placement role (instance, leader or follower) and whether the replica is in sync.

#### Scenario: A partitioned subscription is browsed
- **WHEN** a consumer browses one partition of a subscription from its latest position
- **THEN** only that partition's messages are returned, with a cursor to continue

#### Scenario: Nodes are described one at a time
- **WHEN** a consumer describes a single node of a cluster
- **THEN** only that node is called, so a slow node does not delay the others

### Requirement: Operations are a closed set of commands
Operations SHALL be a closed set of commands: create, update (a merge patch of writable config), delete (if empty, disconnecting consumers), pause and resume (by direction), purge, seek, truncate, close, reassign, elect leader and resolve. System-only operations SHALL be reached through `unwrap`.

#### Scenario: An Artemis-only operation
- **WHEN** a consumer needs to reset an Artemis queue's counters
- **THEN** it reaches it through the driver's unwrapped management interface, not through the neutral operations

### Requirement: Operations are previewed and guarded
- A preview SHALL take the same scope as the execution, and SHALL return the diff, the attributes that require a replace, an effect estimate, an etag and how the precondition is enforced.
- The etag SHALL cover the resolved shard set and each shard's incarnation, not the volume.
- An execution carrying `ifMatch(etag)` SHALL be refused with `FAILED_PRECONDITION` when the etag no longer matches.

#### Scenario: A purge is approved and the queue is recreated
- **WHEN** a purge previewed with an etag runs with `ifMatch` after the queue was deleted and recreated
- **THEN** it is refused and nothing is purged

#### Scenario: A purge is approved and the backlog grows
- **WHEN** a purge previewed at 1,000 messages runs with `ifMatch` after more messages arrived
- **THEN** it runs, and its result states how many messages it removed

#### Scenario: A node joins between preview and execution
- **WHEN** an operation previewed over the whole cluster runs with `ifMatch` after a node joined
- **THEN** it is refused, because the approved scope no longer matches

#### Scenario: Purge and truncate are different effects
- **WHEN** a consumer previews a purge of a subscription and a truncate of its destination
- **THEN** the purge affects only that subscription, and the truncate is reported as affecting every subscription of the destination

### Requirement: Positions, totals and filters are honest
- Positions SHALL be earliest, latest (after the newest message), per-shard offsets, a time, after a cursor, or a snapshot.
- Totals SHALL say whether they count everything or only what matches, and whether they are exact, an estimate or unavailable.

#### Scenario: A lag-based total
- **WHEN** a driver can only derive a backlog from offset distances
- **THEN** the total is reported as an estimate, never as exact

### Requirement: Filters are validated and portable
- A filter SHALL name its dialect.
- A driver SHALL parse a filter in its own dialect before use, and SHALL refuse anything that is not one complete expression with `INVALID_ARGUMENT` and reason `INVALID_FILTER`.
- The portable `openbroker.match` dialect SHALL work on every system, evaluated client side by `openbroker-support` where the broker cannot.

#### Scenario: A filter tries to escape its bound
- **WHEN** a consumer passes the filter `a=1) OR (1=1` to a selection bounded by a production time
- **THEN** the call is refused with `INVALID_FILTER`, and no message is selected

#### Scenario: A portable filter on a broker without selectors
- **WHEN** a consumer counts a subscription's messages with an `openbroker.match` filter on a system without server-side filtering
- **THEN** the count is computed client side and is correct

### Requirement: The message model is complete and typed
A message SHALL carry:
- its standard headers: id, correlation id, reply-to, content type, type, priority, expiry, durability, production time, scheduled delivery time, delivery count, record key, ordering group and deduplication id;
- typed properties;
- namespaced extensions;
- a body that can be streamed.

A truncated body or property set SHALL be marked as truncated. No driver SHALL deserialize a Java object from a message.

#### Scenario: A large message is copied
- **WHEN** a message larger than the memory budget is relayed within one system
- **THEN** its body is streamed and its headers and typed properties arrive unchanged

#### Scenario: An object message is browsed
- **WHEN** a consumer browses a JMS object message
- **THEN** its body is returned as bytes and no class is loaded from it

### Requirement: Transport separates settlement from transactions
Receivers SHALL consume or browse. They SHALL settle messages by accepting, releasing (with an optional delay and a delivery-failed flag) or rejecting (with an optional reason). Transacted batches SHALL be a capability a driver declares, not an assumption, and a commit whose answer never arrived SHALL be reported as in doubt. The transport's capabilities SHALL be static and SHALL be read from the transport itself. The transport API SHALL be versioned like the rest of the API, not marked incubating, and its guarantees SHALL follow the `broker-sdk-messaging` capability.

#### Scenario: A relay on a system without transactions
- **WHEN** a consumer asks a driver without the transacted capability for a transacted sender
- **THEN** the call fails with `UNIMPLEMENTED`, and the transport's capabilities had already said so

#### Scenario: The transport is not incubating
- **WHEN** the API compatibility check runs
- **THEN** it covers `Transport` and every messaging type

### Requirement: Notifications follow a watch contract
- Notifications SHALL use a CloudEvents-shaped envelope (id, source, type, subject, time, data), with native types namespaced.
- The listener SHALL be called serially per node on a driver thread.
- The driver SHALL buffer a bounded number of notifications per node. On overflow, and whenever a stream may have lost notifications, the subscriber SHALL receive a gap telling it to re-list.
- A system without push notifications SHALL offer them through the support library's polling helper.

#### Scenario: A reconnect loses notifications
- **WHEN** a node's notification stream reconnects
- **THEN** the subscriber receives a gap for that node before any new notification

#### Scenario: A slow subscriber
- **WHEN** a subscriber blocks longer than the buffer can absorb
- **THEN** the buffered notifications are dropped and a gap is delivered, and the broker connection is not stalled

### Requirement: Capabilities are static and dynamic
Static capabilities SHALL be the verbs of each kind. Dynamic capabilities SHALL be checked through `Access`, returning available, unavailable or unknown with a reason and, where the broker can be changed to allow it, a remedy.

#### Scenario: A management account cannot write
- **WHEN** the configured account lacks write permission
- **THEN** the write capability is unavailable with the reason and a remedy snippet, and the verbs of the kinds are unchanged

### Requirement: Errors use canonical status codes and reasons
Every failure SHALL be a `BrokerException` carrying:
- a gRPC canonical status;
- an error reason from the SDK's standard set, with a domain and metadata;
- whether the request was not sent, in doubt or rejected;
- the node, where known.

Interruption SHALL surface as `CANCELLED`, and a missed deadline as `DEADLINE_EXCEEDED`. Results across nodes SHALL be partial, stating for each node whether the call applied, failed, is in doubt or was skipped.

#### Scenario: One node of three is down
- **WHEN** a read spans three nodes and one is unreachable
- **THEN** the result holds the two nodes' data and an `UNAVAILABLE` failure for the third, marked not sent

#### Scenario: A deadline across nodes
- **WHEN** a caller binds a 50 ms deadline and reads from three nodes, one of which is slow
- **THEN** the call ends with `DEADLINE_EXCEEDED` within the deadline, on the driver's threads too

### Requirement: Callers control every management and control call
- Drivers SHALL ask the caller's `CallInterceptor` for a permit before every management request and every data-plane control call, and SHALL report the outcome and the broker's response time on that permit.
- The call information SHALL name the node, the endpoint, the plane, a low-cardinality operation name, the cost and the target.
- Per-message sends and receives inside a receiver or sender SHALL NOT be intercepted.

#### Scenario: A rate limit applies inside a batch
- **WHEN** a single facet call makes several broker requests
- **THEN** the interceptor admits each request with its cost and sees each outcome

### Requirement: The caller's context reaches every driver thread
Drivers SHALL wrap every task they hand to another thread with the caller's context propagation, and SHALL carry the caller's deadline onto it.

#### Scenario: An interceptor reads the caller's principal
- **WHEN** a facet call fans out to three nodes on driver threads
- **THEN** the interceptor sees the calling user's context on each of them

### Requirement: Secrets never leak
Values of options marked sensitive and credentials SHALL NOT appear in logs, exception messages, `toString()` output or error metadata. TLS hostname verification SHALL be on by default, redirects SHALL NOT be followed, and management responses SHALL be capped in size.

#### Scenario: A connection fails with a wrong password
- **WHEN** a connection is refused for its credentials
- **THEN** the exception and any log line name the account and the reason, but never the password

### Requirement: Drivers prove conformance
The TCK SHALL test every capability a driver declares against a real broker the driver supplies, and SHALL skip what the driver declares unavailable. It SHALL also cover:
- deadlines;
- redaction;
- filter validation;
- notification overflow;
- preconditions.

#### Scenario: A driver claims a verb it does not honour
- **WHEN** a driver lists `purge` for a kind and a purge leaves messages behind
- **THEN** the TCK fails

### Requirement: The SDK is versioned and released on its own
- The SDK SHALL follow Semantic Versioning, `0.x` until the first stable release, and SHALL be released to Maven Central from a version tag.
- A binary or source break in the API, outside types marked incubating, SHALL require a new minor version while in `0.x`.
- A consumer's release SHALL never depend on an unreleased SDK version.
- Optional modules (`openbroker-opentelemetry`, `openbroker-spring-boot-autoconfigure`, `openbroker-spring-boot-starter`) SHALL be versioned and released with the SDK and listed in the BOM; the benchmark module SHALL NOT be published.

#### Scenario: A breaking change in the API
- **WHEN** a pull request removes a public method of `openbroker-api` without raising the minor version
- **THEN** the API compatibility check fails

#### Scenario: The transport leaves incubation
- **WHEN** the transport types change shape and lose the incubating mark
- **THEN** the release raises the minor version

#### Scenario: The starter is aligned
- **WHEN** an application imports the BOM and adds the starter and a driver without versions
- **THEN** all three resolve to the same SDK release

### Requirement: Credentials take the forms systems use
Credentials SHALL be one of a closed set: a username and password, a bearer token with its expiry, or none (as with mutual TLS). A credential source SHALL be asked for the current credentials on every (re)connect. A driver SHALL refuse a form it cannot use with `INVALID_ARGUMENT`, naming the forms it accepts, and never reveal the secret. A credential source that fails with anything other than a `BrokerException` SHALL reach the driver as `UNAVAILABLE`, not sent, naming the plane and never the failure's text, so the driver backs off and asks again instead of losing the thread that asked.

#### Scenario: A token where a password is expected
- **WHEN** a consumer connects with a bearer token to a driver that accepts only a username and password
- **THEN** the connection fails with `INVALID_ARGUMENT` naming the accepted forms, and the token appears in no message

#### Scenario: A token is refreshed
- **WHEN** a credential source returns a new bearer token before the old one expires
- **THEN** the next connection to a broker uses the new token without closing open links

#### Scenario: A credential source fails
- **WHEN** a credential source throws an exception that is not a `BrokerException` while a driver reconnects a link
- **THEN** the driver sees `UNAVAILABLE`, not sent, without the exception's text, and backs off and retries the link
