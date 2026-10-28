## Why
A consumer that settles a message with a release can say whether the delivery was a failed one, but the SDK does not say whether the system counts the release as a delivery attempt anyway. On a Kafka share group the broker counts every redelivery, so even an uncounted release moves the record towards its delivery limit and, past it, to the dead letter or the archive. On a Kafka consumer group and on Artemis it does not. A consumer cannot tell its users which one they have.

## What Changes
- **BREAKING** `SubscriptionCapabilities` gains a component, `releaseCountsDelivery`: whether a release that is not a failed delivery still counts as a delivery attempt on that kind of subscription.
- The Kafka driver declares it true for share-group subscriptions and false for group subscriptions; the Artemis driver declares it false.
- The messaging TCK checks, for every kind, that the declaration matches what a release does to the redelivered message's delivery count.

## Capabilities
### New Capabilities
None.

### Modified Capabilities
- `broker-sdk-messaging`: the per-kind capabilities include whether a release counts as a delivery.

## Impact
`openbroker-api` (`SubscriptionCapabilities`), `openbroker-kafka`, `openbroker-artemis`, `openbroker-tck`. A caller that constructs `SubscriptionCapabilities` must pass the new component. The next release is `0.6.0`.
