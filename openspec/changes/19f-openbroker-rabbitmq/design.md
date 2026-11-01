# Design: `openbroker-rabbitmq` (change 19f)

Status: design for implementers. OpenBroker part only; the consumer-side part follows the release and is not in this document.
Base: `/home/user/openbroker` at `origin/main` (the checkout is on `fix/kafka-probe-findings`, one Kafka-only commit ahead; branch the worktree off `origin/main`).
Version: the module ships in `0.9.0`. The poms move to `0.9.0-SNAPSHOT` and `openbroker.previous.version` to `0.8.0` (the latest release), so api-check compares against what adopters run and the SDK breaks below are declared by the minor bump.

Library facts were checked against the published artifacts on 2026-11-01 (ctx7 was unreachable through the proxy, so the source jars and the rabbitmq-server sources were read instead):

| Fact | Value / source |
|---|---|
| `com.rabbitmq:amqp-client` | **5.37.0** (latest on Central). Runtime deps: `slf4j-api` (1.7.36, managed to the parent's 2.0.18), `netty-transport/codec/handler` 4.2.18.Final (compile; blocking IO stays the default, Netty only on `factory.netty()`), metrics/micrometer/otel/jackson optional |
| `ConnectionFactory` | `setAutomaticRecoveryEnabled`, `setTopologyRecoveryEnabled`, `useSslProtocol(SSLContext)`, `enableHostnameVerification()`, `setCredentialsProvider`, `setSaslConfig`, `setConnectionTimeout`, `setHandshakeTimeout`, `setChannelRpcTimeout`, `setRequestedHeartbeat`, `setMaxInboundMessageBodySize`, `newConnection(List<Address>, String clientProvidedName)` |
| `Channel` | `confirmSelect`, `getNextPublishSeqNo`, `addConfirmListener(ConfirmCallback ack, ConfirmCallback nack)`, `addReturnListener(ReturnCallback)`, `basicQos(int prefetchCount)`, `basicNack(tag, multiple, requeue)`, `basicReject`, `queueDeclarePassive`, `asyncCompletableRpc(Method)` |
| Exceptions | `AuthenticationFailureException extends PossibleAuthenticationFailureException extends IOException`; `ShutdownSignalException` (RuntimeException) carries the reply code |
| `com.rabbitmq:stream-client` | 1.12.0 (latest). **Not used**, see 1.1 |
| Stream reads over AMQP 0-9-1 | `basic.consume` argument `x-stream-offset` (rabbit_stream_queue.erl l.395); each delivery carries header `x-stream-offset` (l.1517). Stream `purge/1` returns `{error, not_supported}` (l.291) |
| Quorum queue delete | `if-unused` and `if-empty` are refused: "queue.delete operations with if-unused flag set are not supported by quorum queues" (rabbit_quorum_queue.erl l.1117/1121) |
| Queue identity | Management "pid replaced by node"; quorum `INFO_KEYS` = name, durable, auto_delete, arguments, pid, messages…, leader, online, members; stream adds committed_offset, first_timestamp. **No incarnation id is exposed** for any queue type |
| Stream counts | `messages` comes from `queue_coarse_metrics` (sampled), also for passive declare (`stat/1`) |
| Management API | paging on queues, exchanges, connections, channels (`page`, `page_size` ≤ 500, `name`, `use_regex`); `disable_stats=true&enable_queue_totals=true` returns name, vhost, type, node, state, arguments, auto_delete, durable, exclusive, messages, messages_ready, messages_unacknowledged; `DELETE /api/queues/{v}/{n}/contents` purges; `/api/queues/{v}/{n}/get` alters the queue (never used); `X-Reason` on connection DELETE; `/api/overview` has `rabbitmq_version`, `product_name`, `product_version`, `cluster_name`, `listeners`; `/api/nodes` has `applications`, `enabled_plugins`, `partitions`, `running`, alarms |
| Event exchange | topic exchange `amq.rabbitmq.event`, declared in the vhost `event_exchange.vhost` (default `/`), routing keys like `queue.created`, empty body, properties as AMQP 0-9-1 headers |
| Images (Docker Hub) | `rabbitmq:3.13.7-management` (last 3.x), `rabbitmq:4.4.0-management` (newest) |
| Testcontainers | 2.0.5 (already managed); `GenericContainer` is enough, no `testcontainers-rabbitmq` |

---

## 1. Approach

### 1.1 In a few sentences

`openbroker-rabbitmq` reads and changes the cluster through the **management HTTP API** (JDK `HttpClient`, Jackson 3, the same discipline as the Artemis Jolokia client) and moves messages over **AMQP 0-9-1** with `amqp-client` 5.37.0: publisher confirms, manual acks, `basic.qos`. Browse exists only on **streams**, and it is an AMQP 0-9-1 consumer with `x-stream-offset` that acknowledges what it reads, which never removes anything from a stream. Classic and quorum queues declare `BROWSE` unavailable and are never read with get-and-requeue. Recovery is the SDK's `LinkSupervisor` (ADR-0011), so the client's automatic and topology recovery are both off. Events come from `amq.rabbitmq.event` when the plugin and the permissions allow it, otherwise from the polling watch.

**Rejected: the stream protocol client (`stream-client`) for browse.** Its only use here would be reading streams by offset, which AMQP 0-9-1 already does without the `rabbitmq_stream` plugin, a second port (5552), a second endpoint in the connection shape, Netty-native compression libraries (zstd-jni, lz4, snappy) and proton-j. The connection shape therefore has no stream endpoint. Stream-protocol connections, publishers and consumers (`/api/stream/...`) are not listed in this change.

**Rejected: a shared management-HTTP helper extracted into `openbroker-support`.** The Jolokia client (POST batches) and this REST client share only the per-TLS `HttpClient` cache (about 50 lines). Extracting it would touch Artemis for little gain, so `HttpClients` is a small copy of the Artemis `Transports` pattern.

**Rejected: `/api/queues/{v}/{n}/get` with `ack_requeue_true` as browse.** It redelivers, raises delivery counts (quorum queues dead-letter at the limit) and reorders. This is the requirement "Browse never consumes".

### 1.2 Connection shape

| Item | Shape |
|---|---|
| System | `new BrokerSystem("rabbitmq", "RabbitMQ", "[3.13.0,)", 1, Set.of("com.rabbitmq."), List.of())`. No server-side filter dialect, so `filterDialects` is empty |
| Management | `Endpoint(MANAGEMENT, "http" or "https", "host:port[/path-prefix]")`, as cluster endpoints or per node. Any node answers for the whole cluster, so they are tried in order, and the next one is tried only after a `NOT_SENT` failure |
| Data | `Endpoint(DATA, "amqp" or "amqps", "host:port")`. Optional: without one, `transport()` is empty, browse, count and send fail `FAILED_PRECONDITION` with the remedy "add an amqp endpoint", and notifications use polling |
| Nodes | `NodeSpec` ids are free. `Nodes.describe` returns native node names (`rabbit@host`) as `NodeId`, the way Kafka uses broker ids. The TCK fixture names its `NodeSpec`s after the node names, so the counts match |
| Credentials (`CredentialSource.forPlane`) | `UsernamePassword`: HTTP preemptive Basic, AMQP PLAIN. `BearerToken`: HTTP `Authorization: Bearer`, AMQP PLAIN with an empty user name and the token as password (OAuth 2 plugin). `None`: no HTTP header; AMQP SASL `EXTERNAL` (client certificate). Asked again on every connect and every recovery |
| TLS | `TlsProvider.current()` on every connect. HTTP: an `HttpClient` per `TlsMaterial.id()` via `DriverContext.shared` with `SSLParameters` (protocols, cipher suites, endpoint identification `HTTPS` when `hostnameVerification`). AMQP: `useSslProtocol(material.context())` plus `enableHostnameVerification()` when `hostnameVerification`. `amqps`/`https` without material fails `INVALID_ARGUMENT` |
| Options (`RabbitMqOptions.SCHEMA`) | `rabbitmq.watch_interval` (DURATION, default `PT10S`, at least 1 s): polling period and the age of the shared queue listing. `rabbitmq.event_vhost` (STRING, default `/`): where the event exchange lives. Nothing else |
| Supported versions | `[3.13.0,)`. Detected from `GET /api/overview`: `rabbitmq_version` (the gate); `product_name` and `product_version` are kept as node attributes. Below 3.13: `FAILED_PRECONDITION`/`UNSUPPORTED_VERSION` naming the version. A 200 response without `rabbitmq_version`, or one that is not JSON: `FAILED_PRECONDITION`/`UNEXPECTED_SYSTEM`. The version of each node comes from the `rabbit` entry of `/api/nodes[].applications`, so a cluster in a rolling upgrade shows each node's version |

AMQP connections are per **virtual host**: `AmqpConnections` keeps one lazily opened `com.rabbitmq.client.Connection` per vhost. Settings: automatic recovery off, topology recovery off, `clientProvidedName` = `openbroker-<connection id>-<vhost>`, connect and handshake timeouts from `ConnectOptions.timeouts()`, heartbeat 30 s, `maxInboundMessageBodySize` from the receiver's bound. Every connection open and every `link.recover` is a data-plane call admitted by the `CallInterceptor`.

---

## 2. Module layout

`openbroker-rabbitmq/`, an automatic module (`Automatic-Module-Name: io.github.sudoitir.openbroker.rabbitmq`), like Kafka.

### 2.1 `pom.xml`

- Parent `openbroker-parent`. Name "OpenBroker driver for RabbitMQ". Description "Manages and moves messages on RabbitMQ through its management HTTP API and AMQP 0-9-1."
- Dependencies: `openbroker-api`, `openbroker-support`, `com.rabbitmq:amqp-client`, `tools.jackson.core:jackson-databind`. Tests: `openbroker-tck`, `junit-jupiter`, `assertj-core`, `mockito-core`, `testcontainers-junit-jupiter`, `testcontainers` (GenericContainer), `testcontainers-toxiproxy`.
- The Mockito agent `argLine` (`maven-dependency-plugin:properties`, as Kafka).
- Failsafe `systemPropertyVariables`: `rabbitmq.image` and `toxiproxy.image`. Excludes `**/*ChaosIT.java`. Profile `chaos` includes it alone (as Kafka).
- Properties: `rabbitmq.image` = `rabbitmq:4.4.0-management`, `toxiproxy.image` = `ghcr.io/shopify/toxiproxy:2.12.0`.
- JaCoCo `report-aggregate` at `verify` with `includeCurrentProject` true, as Artemis, giving `target/site/jacoco-aggregate/jacoco.xml`.
- `<japicmp.skip>true</japicmp.skip>` with the comment "No release contains this module yet; remove after 0.9.0." The api-check job would otherwise fail to resolve `openbroker-rabbitmq:0.8.0`.

Root `pom.xml`: `<module>openbroker-rabbitmq</module>` after Kafka. `amqp.client.version` 5.37.0 and the `openbroker-rabbitmq` entry go under `dependencyManagement`. Add the coverage path to `sonar.coverage.jacoco.xmlReportPaths` (see 8.4). `openbroker-bom/pom.xml` gets an `openbroker-rabbitmq` entry. `dependencyConvergence` holds because no module combines this driver with Artemis (Netty) in this change.

`src/main/resources/META-INF/services/io.github.sudoitir.openbroker.spi.BrokerDriver` holds the line `io.github.sudoitir.openbroker.rabbitmq.RabbitMqDriver`.

### 2.2 Packages and classes

Base package `io.github.sudoitir.openbroker.rabbitmq`. Every class is `final`. The facets are public, like Kafka's `admin` and `messaging` packages; helpers are package-private.

**`rabbitmq`**
| Class | Responsibility |
|---|---|
| `RabbitMqDriver` | `BrokerDriver`: `SYSTEM = "rabbitmq"`, the description above, `schema()`, `verify` (open, describe nodes, probe access, close), `connect` |
| `RabbitMqOptions` | Protocol constants (`http`, `https`, `amqp`, `amqps`), option keys and defaults, `SCHEMA` |
| `package-info` | the driver's contract in prose: endpoints, credentials per plane, what is unavailable and why |

**`rabbitmq.admin`** (management HTTP API)
| Class | Responsibility |
|---|---|
| `RabbitMqConnection` | `ClusterConnection`: wires the facets; `notifications()` always present; `transport()` present only with a DATA endpoint; `unwrap` offers `ManagementClient` and, with a DATA endpoint, `com.rabbitmq.client.ConnectionFactory` |
| `RabbitMqCluster` | Per-connection context: spec, credentials, `ConnectOptions`, `ManagementClient`, `AmqpConnections`, cached `ClusterFeatures`; `CLUSTER = NodeId.of("cluster")` for cluster-wide calls; `call(operation, Optional<ResourceId>, Deadline, work)`; `reconfigure` drops caches and closes AMQP connections |
| `HttpClients` | One `HttpClient` per TLS material id through `DriverContext.shared` (`Redirect.NEVER`, HTTP/1.1, connect timeout from `Timeouts`) |
| `ManagementClient` | `get/put/post/delete(path, query, body, Deadline, operation)`: JSON via Jackson 3, preemptive auth per request, permit from the `CallInterceptor` and `complete` with the server `Date`, request timeout = time left on the deadline, body read capped at `maxResponseBytes`, endpoint failover on `NOT_SENT`, `Location` header returned for binding creation, never logs bodies or credentials |
| `ManagementPaths` | Percent-encodes vhost and names as path segments (`/` becomes `%2F`; everything outside RFC 3986 unreserved is encoded); maps `""` to `amq.default` and back |
| `Listing` | Paging: server paging for queues, exchanges, connections and channels (`page`, `page_size` = min(limit, 500), `name` + `use_regex=true` from `namePattern`); client paging for the rest; opaque `continueToken` (`base64url("p:<page>")` or `"o:<offset>"`) |
| `RabbitMqErrors` | The whole mapping of section 7 (HTTP, AMQP reply code, IO, TLS), the only place a `BrokerException` is built |
| `ClusterFeatures` | Reads `/api/overview`, `/api/nodes` and `/api/whoami` once per connection; version gate; product; plugins enabled on every node; user tags; feature assessments (section 3) |
| `RabbitMqNodes` | `Nodes.describe/members` from `/api/nodes` and `/api/overview.listeners` (section 2.4) |
| `RabbitMqKinds` | Every `KindId`, the `rabbitmq.*` attribute keys and the `Kind` declarations (label, class, verbs, parent, schema, orderings `Set.of("name")`) |
| `RabbitMqResources` | `Resources.read`: dispatches by kind or class, validates the query (orderings, field selector), runs one listing per kind and merges pages |
| `Vhosts`, `Exchanges`, `Queues`, `Bindings`, `Policies`, `Permissions`, `Users`, `Clients` (connections, channels, consumers), `Shovels`, `FederationLinks` | One reader (and writer, where verbs allow) per family: JSON to `Resource` and attributes back to the request body |
| `Placements` | Queue placements and the `UNDER_REPLICATED` condition (section 2.4) |
| `QueueTypes` | A per-vhost `name → type` map from a reduced listing (`columns=name,vhost,type`, `disable_stats`), kept for `watch_interval`, so bindings, consumers and events can name a queue's kind |
| `RabbitMqOperations` | `Operations.preview/execute` (section 4) |
| `Etags` | A SHA-256 fingerprint of ordered parts; maps sorted (a copy of Kafka's) |
| `RabbitMqAccess` | `Access.check/probe` (section 3) |
| `RabbitMqNotifications` | Chooses the event-exchange stream or the polling watch per subscription; `state()` keyed by `CLUSTER` |
| `EventExchangeStream` | Opens an exclusive server-named queue bound to `amq.rabbitmq.event` with `#` in the event vhost; maps events; reports a `Gap` (`STARTED`, then `RECONNECTED` after every reopen) |
| `EventMapping` | Pure: routing key and headers to `BrokerEvent` (unit-tested without a broker) |
| `ConsumerWatch` | Polling diff of `/api/consumers` that emits `CONSUMER_ATTACHED`/`CONSUMER_DETACHED` with `StandardAttributes.SUBSCRIPTION`, beside `PollingNotifications` for resources (Kafka's `MemberWatch` pattern) |

**`rabbitmq.amqp`** (AMQP 0-9-1 plumbing)
| Class | Responsibility |
|---|---|
| `AmqpFactory` | Builds the `ConnectionFactory` from the spec, credentials and TLS; the DATA address list (cluster endpoints first, then node endpoints) |
| `AmqpConnections` | One connection per vhost, opened lazily under a deadline, closed on `reconfigure` and `close`; reports a connection loss to the links on it |
| `AmqpCalls` | Deadline-bounded control calls on a short-lived channel: `queue.declare` passive, exchange passive (`asyncCompletableRpc(...).get(timeLeft)`) |
| `MessageCodec` | `BasicProperties` and body to and from the SDK `Message` and `OutboundMessage` (section 5.4) |
| `StreamOffsets` | `Position` to the `x-stream-offset` argument; `x-stream-offset` header to `MessageRef.position` |

**`rabbitmq.messaging`** (the `Messages` facet and the transport)
| Class | Responsibility |
|---|---|
| `RabbitMqTransport` | `Transport`: static `capabilities()`, `sender`, `receiver`, `process`, `watch` (a `LinkSupervisor` per link) |
| `RabbitMqSender` | `Sender` on `SendPipeline`: one confirm-mode channel per link, `mandatory` publish, confirms and returns through `ConfirmTracker` |
| `ConfirmTracker` | Publish sequence number to pending receipt; ack (multiple), nack, return-before-ack (`NOT_ROUTED`), channel loss (unconfirmed become in doubt) |
| `RabbitMqReceiver` | Pull `Receiver` on `basicConsume` with manual ack, `basicQos(flow.maxUnsettled())`, a local buffer, settlement, a delayed release held and then nacked |
| `StreamReader` | Browsing `Receiver` and the engine behind `Messages.browse` on a stream: offset start, production-time bound, end at the committed offset at open |
| `RabbitMqLanes` | `ProcessorEngine` lanes over `RabbitMqReceiver` (each lane holds at most one unsettled message) |
| `DeadLetterSink` | The managed-retry dead-letter send with origin headers, through a sender on the target |
| `RabbitMqMessages` | `Messages`: browse (streams only), count, countBy (`UNIMPLEMENTED`), send, bulk (`UNIMPLEMENTED`) |

### 2.3 Kinds

The id path always begins with the vhost. `""` is never a segment: the default exchange is `amq.default`.

| Kind | Class | Id path | Verbs | Notes |
|---|---|---|---|---|
| `rabbitmq.vhost` | NAMESPACE | `[vhost]` | LIST, GET, CREATE, UPDATE, DELETE | `rabbitmq.description`, `rabbitmq.tags`, `rabbitmq.default_queue_type`, limits read-only. Its preview names every queue and its messages, because RabbitMQ deletes the contents of a vhost without asking |
| `rabbitmq.exchange` | DESTINATION | `[vhost, name]` | LIST, GET, CREATE, DELETE, SEND | `rabbitmq.exchange_type`, DURABLE, `rabbitmq.auto_delete`, `rabbitmq.internal`, `rabbitmq.argument.<k>`; all immutable and requiresReplace. `DELIVERY`: `amq.default` is `{POINT_TO_POINT}`, every other exchange `{PUBLISH_SUBSCRIBE}`. `amq.*` cannot be deleted (`FAILED_PRECONDITION`) |
| `rabbitmq.classic-queue` | SUBSCRIPTION | `[vhost, name]` | LIST, GET, CREATE, DELETE, PURGE, SEND, RECEIVE | `BROWSE` and `COUNT` are not declared (COUNT means "counts by a filter", as on Kafka) |
| `rabbitmq.quorum-queue` | SUBSCRIPTION | `[vhost, name]` | LIST, GET, CREATE, DELETE, PURGE, SEND, RECEIVE | `rabbitmq.delivery_limit`, `rabbitmq.leader`, members |
| `rabbitmq.stream` | SUBSCRIPTION | `[vhost, name]` | LIST, GET, CREATE, DELETE, BROWSE, SEND | no PURGE (`not_supported`), no RECEIVE (section 5), `rabbitmq.max_age`, `rabbitmq.max_length_bytes`, `rabbitmq.committed_offset`, `rabbitmq.first_timestamp` |
| `rabbitmq.binding` | ROUTE (BINDING) | `[vhost, exchange, queue, properties_key]` | LIST, GET, CREATE, DELETE | ROUTE_SOURCE = exchange id, ROUTE_TARGET = `Local(queue id of its kind via QueueTypes)`, FILTER = routing key, `rabbitmq.argument.<k>`. Default-exchange bindings are left out (the default exchange's `POINT_TO_POINT` delivery says it) |
| `rabbitmq.exchange-binding` | ROUTE (BINDING) | `[vhost, source, destination, properties_key]` | LIST, GET, CREATE, DELETE | target `Local(exchange id)` |
| `rabbitmq.shovel` | ROUTE (FORWARD) | `[vhost, name]` | LIST, GET, DELETE | From `/api/parameters/shovel` (definition) and `/api/shovels` (state, needs `rabbitmq_shovel_management`). Source and target are `Local` when the URI names this cluster (`amqp://` with no host), else `Remote(uri with the user information removed, queue or exchange)`. Static shovels have MANAGED_BY `configuration` and DELETE fails `FAILED_PRECONDITION` |
| `rabbitmq.federation-link` | ROUTE (LINK) | `[vhost, upstream, type, name]` | LIST, GET | From `/api/federation-links`; DERIVED_FROM names the policy and the upstream; source `Remote(redacted upstream uri, upstream exchange or queue)` |
| `rabbitmq.policy` | POLICY | `[vhost, name]` | LIST, GET, CREATE, UPDATE, DELETE | `rabbitmq.pattern` (the system's own grammar, a regex), `rabbitmq.apply_to`, `rabbitmq.priority`, `rabbitmq.definition.<key>` (JSON value as text) |
| `rabbitmq.operator-policy` | POLICY | `[vhost, name]` | same | `/api/operator-policies` |
| `rabbitmq.permission` | ACCESS_RULE | `[vhost, user]` | LIST, GET, CREATE, UPDATE, DELETE | `rabbitmq.configure`, `rabbitmq.write`, `rabbitmq.read`, PRINCIPAL |
| `rabbitmq.topic-permission` | ACCESS_RULE | `[vhost, user, exchange]` | same | `rabbitmq.write`, `rabbitmq.read` |
| `rabbitmq.user` | PRINCIPAL | `[user]` | LIST, GET | `rabbitmq.tags`, limits. No CREATE (needs a write-only secret, see 9.5); no password hash is ever read into attributes |
| `rabbitmq.connection` | CONNECTION | `[name]` | LIST, GET, CLOSE | PROTOCOL, PRINCIPAL, `rabbitmq.vhost`, CLIENT_ID (`client_properties.connection_name`), REMOTE_ADDRESS, CREATED_AT (`connected_at`), PLACEMENT_NODE, `rabbitmq.channels` |
| `rabbitmq.channel` | CHANNEL | `[name]` | LIST, GET | CONNECTION, `rabbitmq.prefetch_count`, `rabbitmq.unacked`, `rabbitmq.confirm`, `rabbitmq.transactional`, CONSUMER_COUNT. CLOSE is not offered: the management API closes connections only |
| `rabbitmq.consumer` | CONSUMER | `[vhost, channel, consumer_tag]` | LIST | SUBSCRIPTION (queue id of its kind), CHANNEL, `rabbitmq.ack_required`, `rabbitmq.prefetch_count`, `rabbitmq.active`, `rabbitmq.exclusive` |

Not present, and the reason is written in ADR-0033: PARTITION (super streams appear as their exchange and their partition streams; mapping them is a later change); PRODUCER (RabbitMQ lists publishers only for the stream protocol); TRANSACTION (AMQP `tx` is not offered); CONSUMER_GROUP (none). Queues of a type other than classic, quorum and stream (for example the MQTT QoS 0 type) are not listed.

Metrics on queue kinds (from the 19c table): BACKLOG = `messages_ready` (precision ESTIMATE: management statistics are sampled), BACKLOG_SIZE = `message_bytes`, BACKLOG_AGE = now − `head_message_timestamp` when present, IN_FLIGHT = `messages_unacknowledged`, ENQUEUED = `message_stats.publish` (COUNTER with `start` = the node's boot time from `uptime`, so a restart never fakes a rate), ACKNOWLEDGED = `message_stats.ack`, CONSUMER_COUNT = `consumers`. With `disable_stats` (the default for LIST with no metric projection) only BACKLOG and IN_FLIGHT are filled.

### 2.4 Nodes and placements

- `NodeDescription`: node and nativeId = `name`; `Membership(Optional.of(cluster_name), Designation.NONE, running ? ACTIVE : UNKNOWN, Optional.empty())`; functions `{BROKER}`; endpoints = this node's `/api/overview.listeners` (`amqp`, `amqp/ssl`, `http`, `https`, `stream`), as `Endpoint`s with the protocol names of `RabbitMqOptions`; version from `applications[rabbit]`. Conditions: `FULL` while `mem_alarm` or `disk_free_alarm`; `SERVING` false while not running; a node listing network `partitions` gets `OUT_OF_SYNC` with the partitioned nodes named.
- Queue placements (the spec scenario): **quorum and stream**: `leader` → `LEADER`; each other member → `FOLLOWER`; `inSync` = the member is in `online`. **Classic**: `node` → `INSTANCE`. **Classic, mirrored (3.13 only)**: `node` → `LEADER`, `slave_nodes` → `FOLLOWER`, `inSync` from `synchronised_slave_nodes`. `UNDER_REPLICATED` when online < members.
- Shard of a queue: `Shard.node(serving node)` for classic and quorum. A stream is one log: `Shard.partition(PartitionId.of("0"))`, so offsets survive a leader change.

---

## 3. Capabilities (`RabbitMqAccess`)

`probe()` never changes the cluster. It makes one `GET /api/whoami` and reuses `ClusterFeatures`. `check(capability, resource)` is answered from the resource's **kind** where that settles it, with no I/O. This is how the spec scenario "browse is shown as unavailable with the reason, and no message is fetched" is met.

| Capability | Rule |
|---|---|
| `management.read` | AVAILABLE when `/api/whoami` answers. 401 "Login failed": UNAVAILABLE (credentials). 401 "Not management user" or similar: UNAVAILABLE, remedy `rabbitmqctl set_user_tags <user> monitoring` (`text/x-shellscript`). Connection refused, or 404 on `/api/overview`: UNAVAILABLE, remedy `rabbitmq-plugins enable rabbitmq_management`. Anything else: UNKNOWN with the message |
| `management.write` | AVAILABLE when the tags hold `administrator`. With `policymaker` or `management`: UNKNOWN "Writes depend on the vhost's configure permission; the first real call settles it." With `monitoring` only: UNAVAILABLE, remedy: grant `policymaker` or `administrator` |
| `notifications` | Always AVAILABLE when reads are. The reason names the path: "Events from amq.rabbitmq.event in vhost /" or "Polled every PT10S: <why the event exchange is not used>" |
| `messages.browse` | No resource: AVAILABLE "Streams only: classic and quorum queues cannot be read without taking their messages." Resource of kind classic or quorum queue: UNAVAILABLE with that reason and no remedy. Stream: UNKNOWN until the first read (DATA read permission). No DATA endpoint: UNAVAILABLE, remedy "add an amqp endpoint" |
| `messages.send`, `messages.receive` | UNKNOWN "Not tried: the first real call settles it." (or UNAVAILABLE with no DATA endpoint). `receive` on a stream resource: UNAVAILABLE "A stream keeps its messages; browse it." |
| `messages.move`, `messages.copy`, `messages.delete` (SDK constants after 9.3) | UNAVAILABLE "RabbitMQ cannot take one message out of a queue." |
| `rabbitmq.event-exchange` | AVAILABLE when `GET /api/exchanges/<event vhost>/amq.rabbitmq.event` is 200. 404: UNAVAILABLE, remedy `rabbitmq-plugins enable rabbitmq_event_exchange`. The DATA account's rights on it are settled at subscribe; the remedy for a refusal is `rabbitmqctl set_permissions -p <vhost> <user> '^amq\.gen-.*' '^amq\.gen-.*' '^amq\.(gen-.*|rabbitmq\.event)$'` |
| `rabbitmq.shovels` | AVAILABLE when `rabbitmq_shovel` and `rabbitmq_shovel_management` are in `enabled_plugins` on every node; otherwise UNAVAILABLE with `rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management`. Without them, LIST of `rabbitmq.shovel` returns an empty page, never a failure |
| `rabbitmq.federation` | The same with `rabbitmq_federation` and `rabbitmq_federation_management` |
| `rabbitmq.streams` | AVAILABLE on 4.x. On 3.13: from `/api/feature-flags` (`stream_queue` enabled); UNKNOWN when that endpoint is refused (it needs `administrator`) |

---

## 4. Operations, previews and etags (`RabbitMqOperations`)

All previews are `PreconditionEnforcement.CHECKED_BEFORE`: the management API has no conditional request. Execute re-reads the resource, recomputes the etag, refuses `FAILED_PRECONDITION`/`PRECONDITION_FAILED` on a mismatch with every outcome `SKIPPED`, then acts. Outcomes are keyed by `RabbitMqCluster.CLUSTER`.

**Etag** (`Etags.of(...)`, from `GET /api/queues/{v}/{n}`, or the object's GET): kind, vhost, name, `type`, `durable`, `auto_delete`, `exclusive`, sorted `arguments`, and the **member set** (quorum and stream: `members`; classic: `node`; 3.13 mirrors: node plus `slave_nodes`). Never volume, never the leader (a failover is not a change of identity), never `policy` (a policy edit is not a recreation). Exchanges: type, durable, auto_delete, internal, arguments. Policies and permissions: their whole definition. Connections: name plus `connected_at`. `Preview.basis` (gap 9.1) = `CONFIGURATION` for every RabbitMQ etag, because RabbitMQ exposes no incarnation. A queue deleted and created again with the same configuration is therefore not told apart, and the preview says so.

| Operation | Kinds | Call | Preview |
|---|---|---|---|
| `Create(attrs)` | vhost, exchange, the three queue kinds, binding, exchange-binding, policy, operator-policy, permission, topic-permission | `PUT` (bindings: `POST`, the id comes from `Location`) | diff = all attributes. Queues: `x-queue-type` set from the kind; a PUT on an equivalent queue is a no-op (TCK `createIsIdempotent`); an inequivalent one fails `ALREADY_EXISTS`/`RESOURCE_EXISTS` naming the differing argument |
| `Update(patch)` | vhost (description, tags, default queue type), policy, operator-policy, permission, topic-permission | GET, merge, PUT | diff. Queues and exchanges declare no UPDATE: their attributes are immutable and requiresReplace, and the remedy is a policy |
| `Delete(ifEmpty, disconnectConsumers)` | all with DELETE | classic: `DELETE ?if-empty=<ifEmpty>&if-unused=<!disconnectConsumers>`; quorum and stream: checked before (read `messages` and `consumers`, refuse `NOT_EMPTY` or `HAS_CONSUMERS`), then a plain DELETE, because the server refuses those flags. Exchange: `ifEmpty` becomes `if-unused` (refuse `HAS_CHILDREN`). Vhost: `ifEmpty` refuses while it holds a queue | estimate: messages lost (`messages`), consumers cut, bindings removed; vhost: queue and message totals |
| `Purge` | classic, quorum | `DELETE /api/queues/{v}/{n}/contents` | estimate = `messages_ready` (ESTIMATE). The reason says unacknowledged messages stay |
| `Close` | connection | `DELETE /api/connections/{name}` with `X-Reason: Closed by an operator` | the user, client and channel count |
| everything else (Pause, Resume, Seek, Truncate, Reassign, ElectLeader, Resolve) | — | `UNIMPLEMENTED`/`NOT_SUPPORTED`, request not sent, the reason named per verb (no pause in RabbitMQ; streams keep offsets client side over AMQP 0-9-1; member changes and rebalancing have no per-queue management call) | — |

---

## 5. Messaging

### 5.1 Transport capabilities (static, no I/O)

```
guarantees      {AT_MOST_ONCE, AT_LEAST_ONCE}                 // no TRANSACTED: settlement is acknowledgement, AMQP tx is not atomic across queues
deduplication   NONE
orderings       {NONE}
prefetchBounds  {MESSAGES}                                     // RabbitMQ does not implement prefetch-size
subscriptions   classic-queue -> ({NONE}, MESSAGE, false, SystemRetry.NONE, false)
                quorum-queue  -> ({NONE}, MESSAGE, false, new SystemRetry(false, true, true), true)
failover        true                                           // a link reopens on any node of the address list
```
The stream kind is not in `subscriptions`. It offers no consuming receiver or processor (`UNIMPLEMENTED`: "A stream keeps its messages; browse it."), only `ReceiverOptions.browse(...)`. Quorum `SystemRetry`: the queue counts deliveries (`x-delivery-count`) and gives up at its `delivery-limit` (default 20 from 4.0; on 3.13 only when one is set), dead-lettering when a dead-letter exchange is set and otherwise dropping. ADR-0033 records this, and `Processor.retry()` reports it. Ordering is declared `NONE`. Classic single-consumer order holds in practice, but requeue and recovery break it, and declaring it waits for a TCK proof.

### 5.2 Sending (`RabbitMqSender`, `Messages.send`)

- Target: `ToDestination(exchange, routingKey, partition)`: `basicPublish(exchange, routingKey.orElse(""), mandatory=true, props, body)`; a `partition` fails `INVALID_ARGUMENT`. `ToSubscription(queue)`: the default exchange with the queue's name as routing key.
- Opening a sender declares the target passively (`AmqpCalls`), so a missing target fails `NOT_FOUND`/`RESOURCE_GONE` at once instead of closing the channel later.
- `AT_LEAST_ONCE`: `confirmSelect`; `ConfirmTracker` maps `getNextPublishSeqNo` to the pipeline entry. A **basic.ack** confirms; with `multiple`, every entry up to the tag. A **basic.return** (312 NO_ROUTE) before the ack fails that message with `FAILED_PRECONDITION`/`NOT_ROUTED` (gap 9.4, Dispatch `REJECTED`). A **basic.nack** fails it with `RESOURCE_EXHAUSTED`/`DESTINATION_FULL` (`x-overflow: reject-publish`, or a length limit). **Channel or connection loss**: `SendPipeline.suspend`; the `LinkSupervisor` reopens; unconfirmed messages are resent in order with `resent=true` (ADR-0011).
- `AT_MOST_ONCE`: no confirm mode; `publish` returns when it is written; never resent.
- `connection.blocked` (a resource alarm): writes stall and the pipeline's bounded window fills, so `offer` returns full and `send` waits up to its deadline (ADR-0012). The blocked state is reported as a `LinkEvent`.
- `Messages.send` (one message): a per-vhost confirm channel shared by single sends, waiting for the confirm within the call's deadline.

### 5.3 Receiving (`RabbitMqReceiver`, `RabbitMqLanes`)

- `basicQos(flow.maxUnsettled())`; `basicConsume(queue, autoAck = guarantee == AT_MOST_ONCE, tag, consumer)`; deliveries go into a local queue that `receive(n, wait)` drains.
- Settlement maps `Disposition` to AMQP: `Accept` → `basicAck(tag, false)`; `Release(empty, _)` → `basicNack(tag, false, requeue=true)`; `Release(Some(delay), _)` → the delivery is held unacknowledged (still counted against credit) and `basicNack(requeue=true)` runs at its due time on a scheduled virtual thread (TCK `releaseWithDelayIsHiddenUntilDue`); `Reject(reason)` → `basicReject(tag, requeue=false)`, so the queue's dead-letter exchange applies. The managed retry's dead letter uses `DeadLetterSink` with the reason in `MessageHeaders.origin*` and a property. Pull settlement scope is `MESSAGE` (`multiple=false`).
- A delivery tag from a channel that was lost: settling it fails `FAILED_PRECONDITION`/`DELIVERY_STALE` (the channel generation is part of `MessageRef.position`: `"<generation>:<deliveryTag>"`).
- `producedAtOrBefore` on a consuming receiver: `UNIMPLEMENTED` (queues keep no production time). It is supported on stream browse.
- `Retries.resolve(policy, systemRetry)` and `Declared` checks run when a processor or link opens, before any I/O.
- `basic.cancel` from the server (queue deleted, quorum leader moved past the client's tolerance, an exclusive owner gone): reported as a link loss; recovery reopens it or ends `FAILED` with `NOT_FOUND`.

### 5.4 Browse (streams only: `StreamReader`, `Messages.browse`)

- Reachable only for `rabbitmq.stream`. Any other kind: `UNIMPLEMENTED`/`NOT_SUPPORTED` with the reason, before any channel is opened (the spec scenario).
- `Position` → `x-stream-offset`: `Earliest` → `"first"`; `Latest` → `"next"`; `AtOffsets({Shard.partition("0"): n})` → `n` (long); `AtTime(t)` → AMQP timestamp `t` (second granularity, chunk-level, stated in the javadoc); `After(cursor)` → the cursor's offset + 1. A cursor token is `base64url("<offset>|<first_timestamp>")`; when `first_timestamp` changed (stream recreated or truncated past it), the read fails `FAILED_PRECONDITION`, and an offset before the first retained one fails `OUT_OF_RANGE` (19c cursor rule).
- A read: a dedicated channel, `basicQos(min(limit, 500))`, `basicConsume(stream, autoAck=false, args)`, and each delivery acknowledged as it is read (on a stream an ack only returns credit and removes nothing). It stops at `limit`, at the byte bound, or at the end, which is the `committed_offset` read from the management API when the read began, so a browse never waits for new messages. Then `basicCancel` and the channel is closed.
- The production-time bound `producedAtOrBefore(B)`: before reading, a probe consumer at `x-stream-offset = timestamp(B + 1 s)` takes the first offset at or after that time. The reader stops before it; with no such offset it stops at the end.
- `MessageRef(stream id, Shard.partition("0"), "<offset>")`. A portable filter is evaluated client side with `PortableFilter` while reading. A native filter fails `INVALID_ARGUMENT` (no dialect). `BrowsePage.total`: `Total(ALL, estimate(committed_offset − first offset + 1))`, where the first offset is the first delivery of a read from `"first"`, cached per stream for `watch_interval`.

### 5.5 Count (`Messages.count`)

- Classic and quorum: `queue.declare` passive over AMQP gives `ready` exactly at that moment. When management reports `messages_unacknowledged` = 0: `Count.exact(ready)`; otherwise `Count.estimate(ready + unacked)`. With no DATA endpoint: `estimate(messages)` from management (sampled).
- Stream: the offset arithmetic above, as an estimate (coarse metrics are sampled).
- With a filter: `UNIMPLEMENTED`/`NOT_SUPPORTED` ("RabbitMQ cannot count by a filter without taking messages"). `countBy`: `UNIMPLEMENTED`. `bulk`: `UNIMPLEMENTED` for every action and selection, reason as for move.

### 5.6 Message mapping (`MessageCodec`)

| SDK | AMQP 0-9-1 |
|---|---|
| `id` | `message-id` |
| `correlationId`, `replyTo`, `contentType`, `type` | same-named properties |
| `priority` | `priority` (0–255; the queue's `x-max-priority` decides the effect) |
| `expiresAt` | `expiration` = ms from now as a string; a time already past fails `INVALID_ARGUMENT` |
| `durable` | `delivery-mode` 2 or 1; under `AT_LEAST_ONCE` an explicit `false` is refused (TCK `explicitNonDurableUnderAtLeastOnceIsRefused`) |
| `producedAt` | `timestamp` (seconds); set from the clock when absent |
| `deliveryCount` (received) | `x-delivery-count` + 1 where present (quorum); else empty, plus `redelivered` in the `rabbitmq` extension |
| `origin`, `originId` (received) | first `x-death` entry: `exchange`/`queue` and the original `message-id` |
| `key`, `groupId`, `deduplicationId`, `deliverAt` | `INVALID_ARGUMENT` "A RabbitMQ message takes no <x>." (the Artemis pattern) |
| properties | `headers` with typed values (string, long, int, boolean, double, timestamp, bytes); nested tables and arrays become the `rabbitmq` extension, never Java-deserialized |
| body | bytes as they are; `content-encoding` is kept as a property; larger than `maxBodyBytes`: truncated with `MessagePart.BODY` |
| extension `rabbitmq` | `exchange`, `routing_key`, `redelivered`, `delivery_tag`, `x-stream-offset`, `user-id`, `app-id` |

### 5.7 Recovery and observability

- `ConnectionFactory`: `setAutomaticRecoveryEnabled(false)` and `setTopologyRecoveryEnabled(false)` (ADR-0011: "The broker client's own failover is not used"). A `ShutdownListener` on each connection and channel reports a loss to the `LinkSupervisor`, which reopens through `AmqpConnections` with the recovery's backoff, asks `CredentialSource` and `TlsProvider` again, re-declares nothing (the driver never declares topology on behalf of a link), and ends `OPEN` with `servedBy` = the node in `connection.getServerProperties()` / `cluster_name`, or `FAILED` after `giveUpAfter`.
- Telemetry goes only through the SDK hooks (ADR-0015): `MessagingObserver.onSend` writes the carrier into AMQP `headers` (string values); `onReceive`, `onProcess` and `onSettle` come through `EngineContext`. The client's `MetricsCollector` and `ObservationCollector` stay unset. Every HTTP request and AMQP control call goes through `CallInterceptor` (`permit` then `complete`).

---

## 6. Events (`RabbitMqNotifications`)

- **Event exchange** (`rabbitmq.event-exchange` available, and the DATA account allowed): one `EventExchangeStream` per subscription, on the event vhost's AMQP connection. It declares an exclusive, auto-delete, server-named queue, binds it to `amq.rabbitmq.event` with `#`, and consumes with auto-ack. On open, the dispatcher gets `Gap(CLUSTER, now, STARTED)`; after a recovery, `RECONNECTED`, because events in between are lost. `state()` = `{CLUSTER: phase}`. Serial delivery and overflow come from `NotificationDispatcher`.
- `EventMapping` (the subject id is resolved through `QueueTypes` for queue kinds; it is primed by one reduced listing when the stream opens and updated on `queue.created`):

| Routing key | BrokerEvent |
|---|---|
| `queue.created` / `queue.deleted` | `RESOURCE_CREATED` / `RESOURCE_DELETED`, subject = queue id of its kind (from `arguments.x-queue-type`, else the vhost's default queue type, else `QueueTypes`) |
| `exchange.*`, `binding.*`, `vhost.*`, `policy.set`/`policy.cleared`, `permission.created`/`permission.deleted`, `user.*`, `parameter.*` (shovel) | created / deleted of the matching kind |
| `consumer.created` / `consumer.deleted` | `CONSUMER_ATTACHED` / `CONSUMER_DETACHED`, data `SUBSCRIPTION` = the queue id (TCK `aConsumerAttachedEvent…`), `CONSUMER`, `CHANNEL` |
| `connection.created` / `connection.closed` | `CLIENT_CONNECTED` / `CLIENT_DISCONNECTED`, PRINCIPAL, PROTOCOL, CLIENT_ID |
| `channel.created` / `channel.closed` | `CHANNEL_OPENED` / `CHANNEL_CLOSED` |
| `alarm.set` / `alarm.cleared`, `node.*` | `rabbitmq.node.alarm` / `rabbitmq.node.<x>` (namespaced) |
| anything else | `rabbitmq.<routing key>` with the headers as namespaced attributes |

Event time = `timestamp_in_ms` header; id = `<routing key>:<timestamp>:<sequence>`. The `user_who_performed_action` header becomes PRINCIPAL.

- **Polling** (plugin absent, `ACCESS_REFUSED` on the event vhost, or no DATA endpoint): `PollingNotifications` over queries for vhosts, exchanges, the three queue kinds, bindings, policies, permissions, connections and channels every `watch_interval`, plus `ConsumerWatch` for attached and detached. The choice is made per subscribe; a refusal of the event exchange falls back once, and that is stated in the `notifications` assessment.

---

## 7. Error mapping (`RabbitMqErrors`)

Management HTTP (body `{"error": e, "reason": r}`):

| Condition | Status | Reason | Dispatch |
|---|---|---|---|
| connect refused, unknown host, no route | UNAVAILABLE | UNREACHABLE | NOT_SENT |
| `SSLHandshakeException`, certificate or host name mismatch | UNAVAILABLE | TLS_HANDSHAKE_FAILED | NOT_SENT |
| `HttpTimeoutException`, deadline passed | DEADLINE_EXCEEDED | — | NOT_SENT before the request was written, else IN_DOUBT |
| 401 with r = "Login failed" (or no body) | UNAUTHENTICATED | CREDENTIALS_REJECTED (+ `openbroker.plane`) | REJECTED |
| 401 with any other reason ("Not management user", "Not administrator user", "Access refused."), 403 | PERMISSION_DENIED | NOT_AUTHORIZED (+ `openbroker.resource` when known) | REJECTED |
| 404 on `/api/overview` | NOT_FOUND | ENDPOINT_NOT_FOUND | REJECTED |
| 404 on an object | NOT_FOUND | RESOURCE_GONE (+ resource) | REJECTED |
| 400 with r containing "inequivalent arg" | ALREADY_EXISTS | RESOURCE_EXISTS | REJECTED |
| 400 with r containing "not empty" / "in use" | FAILED_PRECONDITION | NOT_EMPTY / HAS_CONSUMERS | REJECTED |
| 400 other (`bad_request`) | INVALID_ARGUMENT | INVALID_ARGUMENT | REJECTED |
| 405 | UNIMPLEMENTED | NOT_SUPPORTED | REJECTED |
| 3xx (redirects are not followed) | FAILED_PRECONDITION | ENDPOINT_NOT_FOUND ("the management endpoint redirects; give the final URL") | REJECTED |
| 429 | RESOURCE_EXHAUSTED | RATE_LIMITED | REJECTED |
| 500 | INTERNAL | BAD_RESPONSE | IN_DOUBT for writes, REJECTED for reads |
| 502, 503, 504 | UNAVAILABLE | UNREACHABLE | IN_DOUBT for writes |
| body over `maxResponseBytes` | RESOURCE_EXHAUSTED | RESPONSE_TOO_LARGE | IN_DOUBT |
| 200 that is not JSON, or JSON of the wrong shape | INTERNAL | BAD_RESPONSE | IN_DOUBT |
| 200 on `/api/overview` without `rabbitmq_version` | FAILED_PRECONDITION | UNEXPECTED_SYSTEM | REJECTED |
| version below 3.13 | FAILED_PRECONDITION | UNSUPPORTED_VERSION | REJECTED |
| the interceptor refuses a permit | as the interceptor says | CALL_REFUSED | NOT_SENT |

AMQP 0-9-1 (`ShutdownSignalException.getReason()` reply code, `basic.return`, client exceptions):

| Code / condition | Status | Reason | Dispatch |
|---|---|---|---|
| `AuthenticationFailureException`, `PossibleAuthenticationFailureException` | UNAUTHENTICATED | CREDENTIALS_REJECTED (+ plane) | REJECTED |
| 530 NOT_ALLOWED (vhost access at `connection.open`) | PERMISSION_DENIED | NOT_AUTHORIZED (+ vhost) | REJECTED |
| 403 ACCESS_REFUSED | PERMISSION_DENIED | NOT_AUTHORIZED (+ `openbroker.resource` parsed from the reply text: queue or exchange id) | REJECTED |
| 404 NOT_FOUND | NOT_FOUND | RESOURCE_GONE (+ resource) | REJECTED |
| 405 RESOURCE_LOCKED (exclusive queue, exclusive consumer) | FAILED_PRECONDITION | HAS_CONSUMERS | REJECTED |
| 406 PRECONDITION_FAILED, "inequivalent arg" | ALREADY_EXISTS | RESOURCE_EXISTS | REJECTED |
| 406 "delivery acknowledgement … timed out" / unknown delivery tag | FAILED_PRECONDITION | DELIVERY_STALE | REJECTED |
| 406 other | FAILED_PRECONDITION | PRECONDITION_FAILED | REJECTED |
| 312 NO_ROUTE (`basic.return`) | FAILED_PRECONDITION | NOT_ROUTED (new, 9.4) | REJECTED |
| `basic.nack` | RESOURCE_EXHAUSTED | DESTINATION_FULL | REJECTED |
| 320 CONNECTION_FORCED, socket closed, missed heartbeats | UNAVAILABLE | UNREACHABLE | IN_DOUBT (unconfirmed sends) |
| 506 RESOURCE_ERROR | RESOURCE_EXHAUSTED | — (`openbroker.io` domain, no specific reason) | REJECTED |
| 540 NOT_IMPLEMENTED | UNIMPLEMENTED | NOT_SUPPORTED | REJECTED |
| 501, 502, 503, 504, 505 (frame and syntax errors) | INTERNAL | BAD_RESPONSE | IN_DOUBT |
| 541 INTERNAL_ERROR | INTERNAL | — | IN_DOUBT |
| `ConnectException`, `UnknownHostException`, `NoRouteToHostException` at open | UNAVAILABLE | UNREACHABLE | NOT_SENT |
| `SSLHandshakeException` at open | UNAVAILABLE | TLS_HANDSHAKE_FAILED | NOT_SENT |
| `TimeoutException` (handshake, RPC, confirm wait) | DEADLINE_EXCEEDED | — | NOT_SENT at open, IN_DOUBT after |

No message, `toString()` or metadata ever holds a password, a token or a URI's user information. `RabbitMqErrorsTest` checks that with a secret marker.

---

## 8. Tests

### 8.1 Unit tests (run locally, no Docker)

`ManagementClientTest` and every facet test with a management call use **`FakeManagement`** (test helper, `com.sun.net.httpserver.HttpServer` on port 0 with canned JSON from `src/test/resources/io/github/sudoitir/openbroker/rabbitmq/admin/*.json`, recording the requests). AMQP-side tests mock `com.rabbitmq.client.Channel` and `Connection` with Mockito (the agent is already wired).

| Test class | Proves |
|---|---|
| `RabbitMqDriverTest` | ServiceLoader finds the driver; `system()`; schema options; bad `watch_interval` refused |
| `ManagementClientTest` | Basic and Bearer headers; no redirect followed; body cap; timeout → DEADLINE_EXCEEDED; refused → UNREACHABLE/NOT_SENT and failover to the second endpoint; one permit per request; no secret in failures |
| `ManagementPathsTest` | `/`, `%`, `#`, spaces, unicode, `amq.default` |
| `RabbitMqErrorsTest` | Every row of both tables in section 7 |
| `ClusterFeaturesTest` | Version parse and floor; product; plugins on all nodes versus some; tags |
| `RabbitMqNodesTest` | Node descriptions, alarms, partitions, listeners to endpoints |
| `RabbitMqKindsTest` | Every kind: class, label, verbs; stream has no PURGE or RECEIVE; classic and quorum have no BROWSE |
| `PlacementsTest` | Quorum leader and two followers (the spec scenario, from a fixture); a member offline → inSync false and UNDER_REPLICATED; 3.13 mirrors |
| `RabbitMqResourcesTest` | Query validation (undeclared ordering → INVALID_ARGUMENT), server and client paging tokens, name regex, every family's mapping from fixtures |
| `RabbitMqOperationsTest` | Etag stable when the backlog grows, changed by arguments or member set; mismatch → FAILED_PRECONDITION with nothing called; quorum delete flags checked before; `X-Reason` on close; unsupported operations not sent |
| `RabbitMqAccessTest` | The section 3 matrix, including the per-resource browse answer with zero requests |
| `EventMappingTest` | Every routing-key row of section 6, including consumer.created → CONSUMER_ATTACHED with SUBSCRIPTION |
| `ConsumerWatchTest` | Attach and detach diffs |
| `AmqpFactoryTest` | Recovery off (both kinds), TLS and hostname verification, EXTERNAL for `None`, bearer as password, address order |
| `MessageCodecTest` | Every row of 5.6; refused headers |
| `StreamOffsetsTest` | Every `Position`; cursor encode, decode and staleness |
| `ConfirmTrackerTest` | Ack with multiple, nack, return before ack, loss → resend set |
| `RabbitMqReceiverTest` | qos, ack, nack, reject, delayed release, stale tag after a channel generation change |
| `RabbitMqTransportCapabilitiesTest` | The static capabilities of 5.1 |

Local command, one at a time and in a capped scope:
```bash
export JAVA_HOME=/opt/jdk25 PATH=/opt/jdk25/bin:$PATH
cd <worktree>
systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=2G -- \
  ./mvnw -q -pl openbroker-rabbitmq -am test -Dtest='<Class>' -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -q -pl openbroker-rabbitmq process-sources   # Palantir format before committing
```

### 8.2 Integration tests (Testcontainers, CI only: this machine has no Docker daemon)

Fixture **`RabbitMqTestCluster`** (test sources): three `GenericContainer`s of `rabbitmq.image` on one network, aliases `rabbit-1..3`, hostnames set, `RABBITMQ_NODENAME=rabbit@rabbit-N`, a shared Erlang cookie (prove whether the image still honours `RABBITMQ_ERLANG_COOKIE` on 3.13.7 and 4.4.0; otherwise copy `/var/lib/rabbitmq/.erlang.cookie` with mode 0400 before start), `rabbitmq.conf` with `cluster_formation.peer_discovery_backend = classic_config` and the three nodes, `enabled_plugins` = `[rabbitmq_management,rabbitmq_event_exchange,rabbitmq_shovel,rabbitmq_shovel_management,rabbitmq_federation,rabbitmq_federation_management].`, wait for `/api/health/checks/is-in-service` on each node and for three running nodes in `/api/nodes`. `shared()` per JVM (as Kafka). `spec()` names the `NodeSpec`s `rabbit@rabbit-N` with each node's mapped management and AMQP ports. **`RabbitMqTestCluster.single(plugins…)`** gives one node with management only, for the fallback tests. Admin helpers (create user with permissions, kill a connection) live in `RabbitMq.java` (the `Kafka.java` pattern).

| IT | Proves |
|---|---|
| `RabbitMqConnectionIT` | verify, nodes and versions on both images; wrong password; unreachable; management-only connection (no DATA endpoint) |
| `RabbitMqResourcesIT` | Every kind listed and its CRUD; **quorum queue on three nodes names leader and two followers** (spec scenario); bindings with the right target kind; shovel and federation link read from a configured shovel and upstream |
| `RabbitMqOperationsIT` | Purge, delete with flags on classic and quorum, etag refusals, vhost delete preview, close a connection |
| `RabbitMqWatchIT` | Event-exchange events, and polling on `single()` without the plugin, including CONSUMER_ATTACHED |
| `RabbitMqBrowseIT` | Stream browse by first, offset, time and cursor, with bytes and limit bounds; **classic queue browse unavailable and nothing fetched** (spec scenario: `message_stats.deliver_get` and `messages_ready` unchanged, no consumer ever registered) |
| `RabbitMqMessagingIT` | Confirms, nack with `reject-publish`, NOT_ROUTED, delayed release, recovery after the connection is closed through the management API, `resent=true` |
| `RabbitMqTlsIT` | `https` and `amqps` with `TestPki`-made certificates; hostname verification on; mutual TLS with EXTERNAL (provides `mutualTlsRequest` to the messaging TCK) |
| `RabbitMqTckIT` | `OpenBrokerTck` with subscription kind `rabbitmq.quorum-queue` |
| `RabbitMqClassicTckIT` | `OpenBrokerTck` with `rabbitmq.classic-queue` |
| `RabbitMqStreamTckIT` | `OpenBrokerTck` with `rabbitmq.stream` (`browseShard` = `Shard.partition("0")`); runs the browse cases |
| `RabbitMqMessagingTckIT` | `MessagingTck`, `subscriptionKinds()` = classic and quorum, `restrictedRequest()` (a user limited to `^tck\.allowed\..*`), `mutualTlsRequest()` from the TLS fixture |
| `RabbitMqChaosIT` | `ChaosTck` (profile `chaos`): Toxiproxy in front of each node's AMQP port; `serving` = quorum leader, `successor` = an online follower; kill, cut and heal |

`RabbitMqTckEnvironment` implements `TckEnvironment`: `subscriptionId(name)` = `[ "/", name ]` of the chosen kind; `subscriptionAttributes` = durable true, plus `x-queue-type` from the kind; `nativeEquals` and `nativeDialect`: not reached (no COUNT verb); `requestToAddress(host, port)` = a management endpoint at that address; `requestWithWrongPassword`; `competingReceivers()` true.

### 8.3 CI matrix (`.github/workflows/ci.yml`)

- `verify`: add `{ driver: rabbitmq, image: "rabbitmq:3.13.7-management" }` and `{ driver: rabbitmq, image: "rabbitmq:4.4.0-management" }`; step `if: matrix.driver == 'rabbitmq'` → `./mvnw -B verify -pl openbroker-rabbitmq -am -Drabbitmq.image=${{ matrix.image }}`. **Add `!openbroker-rabbitmq` to the Artemis legs' `-pl` exclusion list**, or the Artemis legs run the RabbitMQ ITs. Upload `openbroker-rabbitmq/target/site/jacoco-aggregate` as `coverage-rabbitmq` when the image is `rabbitmq:4.4.0-management`.
- `chaos`: add both RabbitMQ images (ADR-0017: oldest and newest); step `./mvnw -B -pl openbroker-rabbitmq -am verify -Pchaos -Dit.test=RabbitMqChaosIT -Drabbitmq.image=${{ matrix.image }}`.
- `sonar`: download `coverage-rabbitmq` into `openbroker-rabbitmq/target/site/jacoco-aggregate`.
- `ci-ok` is unchanged (the jobs it needs are the same).

### 8.4 Coverage and Sonar

Root `sonar.coverage.jacoco.xmlReportPaths` gets `${maven.multiModuleProjectDirectory}/openbroker-rabbitmq/target/site/jacoco-aggregate/jacoco.xml` (comment updated to "the Artemis, Kafka and RabbitMQ drivers' aggregates…"). The driver's aggregate includes its unit tests and ITs (the failsafe exec from `prepare-agent-it`). New code reaches 80% because every facet has both a unit test against fixtures and an IT. The paths that only a real broker exercises (recovery, chaos) are covered on CI.

---

## 9. SDK gaps (fixed in the SDK for every driver, before the driver uses them)

1. **An etag cannot always name an incarnation.** RabbitMQ exposes no queue, exchange or policy incarnation (management replaces pids by nodes), so `Preview.etag`'s promise ("a resource recreated since … refuses the operation") cannot be kept. Fix: `operation/EtagBasis { INCARNATION, CONFIGURATION }` and a `Preview.basis` component. `INCARNATION`: the etag holds a system-assigned identity, so a recreated resource is refused. `CONFIGURATION`: the system exposes none; the etag holds the shard set and the configuration, so a resource created again with the same configuration is not told apart. Artemis (queue id) and Kafka topics (topic id) declare `INCARNATION`; each other Kafka call site is judged by what its etag holds (ACLs are `CONFIGURATION`). TCK `anEtagRefusesARecreatedResourceButNotAGrownBacklog` keeps the grown-backlog half for all, and asserts the recreate refusal only when `basis == INCARNATION` (`assumeTrue` with the reason otherwise). ADR-0034. Breaking (a new record component), declared by the 0.9 minor bump.
2. **TCK cases that ignore the kind's verbs.** `aReceiverBoundedByProductionTimeReadsOnlyWhatWasProducedBefore` and `aBrowsingReceiverLeavesTheMessagesWhereTheyAre` open browsing receivers without `assumeVerb(Verb.BROWSE)`. `aConsumerAttachedEventNamesItsSubscriptionUnderTheStandardKey` opens a consuming receiver without `assumeVerb(Verb.RECEIVE)`. On a kind that honestly lacks them they fail instead of being skipped. Fix: add the assumptions. Artemis and Kafka declare both verbs, so nothing changes for them.
3. **Message-action capability ids live in one driver.** `messages.move`, `messages.copy` and `messages.delete` are defined privately in `KafkaAccess`. Fix: `Capability.MOVE_MESSAGES`, `COPY_MESSAGES` and `DELETE_MESSAGES` in `openbroker-api`, and `KafkaAccess` uses them. Additive.
4. **A send the system accepted but routed nowhere has no reason.** With `mandatory`, RabbitMQ returns it (312 NO_ROUTE); Artemis drops it, and an SNS or Service Bus topic with no subscription behaves alike. Fix: `ErrorReasons.NOT_ROUTED` ("the system took the message and no subscription received it"), with status `FAILED_PRECONDITION`. ADR-0035 says when a driver must report it (whenever the system tells it). Additive.
5. **Deferred, not fixed in this change: write-only secret attributes.** Creating a shovel or a federation upstream (URIs with passwords) or a user (password) needs an attribute the SDK accepts on write and never returns or diffs. Until then those kinds declare no CREATE, which is an honest unavailability, not a workaround. See the open questions.

---

## 10. Task list (file-disjoint units for parallel implementers)

Rules for every implementer: read narrowly (grep first, then only the needed lines); run one Maven command at a time, in the capped scope of 8.1; at most two heavy commands across all agents; never mention a consuming product. Commit on the unit's branch with Conventional Commits; do not open PRs. ITs cannot run locally: write them, compile them (`-DskipITs` and `test-compile`), and leave the run to CI.

**Wave 0 (lead, inline):** `openbroker/openspec/changes/19f-openbroker-rabbitmq/` (proposal, design from this document, `specs/broker-sdk-rabbitmq/spec.md` with the three requirements sharpened, `specs/broker-sdk/spec.md` delta for gaps 1–4, tasks). ADR-0033 "The RabbitMQ mapping: browse only on streams, settlement by acknowledgement", ADR-0034 "An etag states whether it names an incarnation", ADR-0035 "A send routed nowhere fails NOT_ROUTED". Files: `openspec/changes/19f-*/**`, `docs/adr/0033-0035*`.

| # | Unit | Files it owns | Depends on | Finish line (local) | CI proof |
|---|---|---|---|---|---|
| T1 | SDK gaps 1–4 | `openbroker-api/**/operation/{EtagBasis,Preview}.java`, `access/Capability.java`, `ErrorReasons.java`, `openbroker-tck/**/OpenBrokerTck.java`, the 7 Kafka `new Preview` sites plus `KafkaAccess.java`, `ArtemisOperations.java`, new `openbroker-api/src/test/**/operation/PreviewTest.java` | — | `./mvnw -q -pl openbroker-api,openbroker-kafka,openbroker-artemis -am test -Dtest='PreviewTest,KafkaDriverTest,KafkaErrorsTest' -Dsurefire.failIfNoSpecifiedTests=false` green and the whole reactor compiles | Kafka and Artemis TCK legs green |
| T2 | Build skeleton | root `pom.xml` (module, dependencyManagement, Sonar path), `openbroker-bom/pom.xml`, `openbroker-rabbitmq/pom.xml`, `META-INF/services/…BrokerDriver`, `RabbitMqDriver`, `RabbitMqOptions`, all `package-info.java`, **stubs** (public constructors and signatures fixed by this document, bodies throwing `UNIMPLEMENTED`) for every class of 2.2 | — (runs beside T1) | `RabbitMqDriverTest` | — |
| T3 | Management foundation | `admin/{RabbitMqCluster,HttpClients,ManagementClient,ManagementPaths,Listing,RabbitMqErrors,ClusterFeatures,RabbitMqNodes,RabbitMqKinds,QueueTypes,Etags}.java`, `amqp/AmqpReplies` part of `RabbitMqErrors` only, test helper `FakeManagement.java`, their fixtures | T2 | `ManagementClientTest`, `ManagementPathsTest`, `RabbitMqErrorsTest`, `ClusterFeaturesTest`, `RabbitMqNodesTest`, `RabbitMqKindsTest` | `RabbitMqConnectionIT` |
| T4 | Resources | `admin/{RabbitMqResources,Vhosts,Exchanges,Queues,Bindings,Policies,Permissions,Users,Clients,Shovels,FederationLinks,Placements}.java` and fixtures | T3 | `RabbitMqResourcesTest`, `PlacementsTest` | `RabbitMqResourcesIT` |
| T5 | Operations | `admin/RabbitMqOperations.java`, the writer halves it needs as package-private methods in a new `admin/Writes.java` (not in T4's files) | T1, T3 | `RabbitMqOperationsTest` | `RabbitMqOperationsIT` |
| T6 | Access and notifications | `admin/{RabbitMqAccess,RabbitMqNotifications,EventExchangeStream,EventMapping,ConsumerWatch}.java` | T1, T3; `EventExchangeStream` uses T7's `AmqpConnections` interface from the T2 stub | `RabbitMqAccessTest`, `EventMappingTest`, `ConsumerWatchTest` | `RabbitMqWatchIT` |
| T7 | AMQP plumbing | `amqp/{AmqpFactory,AmqpConnections,AmqpCalls,MessageCodec,StreamOffsets}.java` | T3 | `AmqpFactoryTest`, `MessageCodecTest`, `StreamOffsetsTest` | — |
| T8 | Transport | `messaging/{RabbitMqTransport,RabbitMqSender,ConfirmTracker,RabbitMqReceiver,RabbitMqLanes,DeadLetterSink}.java` | T1, T7 | `ConfirmTrackerTest`, `RabbitMqReceiverTest`, `RabbitMqTransportCapabilitiesTest` | `RabbitMqMessagingIT` |
| T9 | Messages facet | `messaging/{RabbitMqMessages,StreamReader}.java` | T7 (and `RabbitMqSender` from T8 for `send`; until T8 lands, `send` uses `AmqpCalls` directly) | `RabbitMqMessagesTest` (mocked channel: browse refused on classic with zero channel calls; offsets; count rules) | `RabbitMqBrowseIT` |
| T10 | Test cluster fixture | `src/test/**/rabbitmq/{RabbitMqTestCluster,RabbitMq,TestPki}.java`, `src/test/resources/rabbitmq/{rabbitmq.conf,enabled_plugins}` | T2 | test-compile green | every IT |
| T11 | Connection wiring and TCK | `admin/RabbitMqConnection.java`, `src/test/**/{RabbitMqTckEnvironment,RabbitMqTckIT,RabbitMqClassicTckIT,RabbitMqStreamTckIT,messaging/RabbitMqMessagingTckIT,RabbitMqTlsIT}.java` | T4–T10 | test-compile green; `RabbitMqDriverTest` still green | all four TCK ITs on both images |
| T12 | Chaos | `src/test/**/RabbitMqChaosIT.java`, `RabbitMqChaosCluster.java` | T8, T10 | test-compile green | `chaos (rabbitmq:…)` legs |
| T13 | CI, docs, finish | `.github/workflows/ci.yml`, `README.md`, `CLAUDE.md` layout table (one row: "`openbroker-rabbitmq` | The RabbitMQ driver (management HTTP API + AMQP 0-9-1) | api, support, amqp-client, Jackson 3"), the TCK and driver sections of the README | T11 | `./mvnw -q -DskipTests verify` green locally (format, enforcer, javadoc) | the PR's `ci-ok` green, Sonar's issue list empty, coverage on new code ≥ 80% |

Order: **T1 ∥ T2** → **T3 ∥ T10** → **T4 ∥ T5 ∥ T6 ∥ T7** → **T8 ∥ T9** → **T11 ∥ T12** → **T13**. Integration tests first run when the lead merges the units into the session branch and pushes (one PR in OpenBroker, auto-merge on green). A red IT goes back to the unit that owns the file.

Finish line for the whole change: the OpenBroker PR's CI is green, including `verify (rabbitmq:3.13.7-management)`, `verify (rabbitmq:4.4.0-management)`, both `chaos (rabbitmq:…)` legs and `api-check`. The release is then cut by a maintainer.

---

## 11. Open questions for the user

1. **Version floor.** `[3.13.0,)` covers estates still on the last 3.x line (community support ended; it keeps classic mirroring, which is modelled). `[4.0.0,)` would drop the mirror placements and the 3.13 CI legs. Default: 3.13.
2. **Shovel, federation-upstream and user creation.** They need a write-only secret attribute in the SDK (gap 9.5), a larger API change. Default: not in this change; those kinds are read and delete only, and the gap becomes its own roadmap change.

Defaulted without asking: no stream-protocol client or endpoint; super streams as PARTITION later; no RabbitMQ benchmarks in `openbroker-benchmarks` (ADR-0016 does not require a per-driver budget; it can follow); no Spring Boot connection-details deriver for RabbitMQ in this change.
