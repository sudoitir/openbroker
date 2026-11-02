## Why
Operators tune a Kafka cluster through broker settings: message size limits, cleaner and network threads, retention
defaults. The SDK has no resource for settings that belong to a node or to the whole cluster, so a consumer has no
safe way to see them or to change a dynamic one. RabbitMQ runtime parameters are the same kind of thing.

## What Changes
- **BREAKING (SDK)**: `ResourceClass.CONFIGURATION`, the settings of a node or of the whole cluster. Exhaustive
  `switch`es over `ResourceClass` must add a case. Minor version bump while 0.x; japicmp is unaffected, because an
  added enum constant is not a binary break.
- The Kafka driver declares `kafka.broker-config` (class `CONFIGURATION`, verbs `LIST`, `GET`, `UPDATE`): one
  resource per broker, and `*` for the cluster-wide default that Kafka stores under an unnamed broker resource.
- A listing reads a page of configurations with one `describeConfigs`. Settings are `kafka.config.<name>`
  attributes, never a secret. `kafka.config_read_only` names the settings only a restart changes and
  `kafka.config_sources` says where each overridden setting comes from.
- `UPDATE` previews a before and after of the patched settings with an etag, then applies them with
  `incrementalAlterConfigs`. A setting that needs a restart, or holds a secret, is refused before anything is sent.
- A cluster-authorization refusal is reported as `NOT_AUTHORIZED`, naming what the account needs.

## Capabilities
### Modified Capabilities
- `broker-sdk`: the `CONFIGURATION` resource class.
- `broker-sdk-kafka`: the broker-configuration kind.

## Out of scope
- RabbitMQ runtime parameters, and Artemis, which hold no such class yet.
- Topic and client-quota configuration, and sensitive settings, which need write-only attributes.

## Impact
`openbroker-api` (`ResourceClass`), `openbroker-artemis` (its class-to-kind switch), `openbroker-kafka`
(`BrokerConfigs`, `KafkaKinds`, `KafkaErrors`).
