# Tasks

Archived 2026-11-27. Sections 1 and 2 are done: the driver work merged and the release that contains
it is published. The product-side tasks were completed in the product repository, which owns them.

## 1. OpenBroker

- [x] 1.1 Report every replica's broker, in-sync flag, byte size and offset lag from one `describeLogDirs` for the brokers of the page. A broker that does not answer is named and the other replicas are still returned. Verify with a driver test for a follower 40 offsets behind, and a test where one of three brokers fails the log-directory read.
- [x] 1.2 Add the tiered-offset capability. On success, report local log start and tiered offset. On `UnsupportedVersionException`, declare the capability unavailable with the broker's reason and do not retry that spec on the connection. On a topic that is not tiered, report local start equal to log start and tiered offset unknown with the reason "not tiered". Verify with a unit test for the refusal and a test against a current KRaft image for a topic that is not tiered.
- [x] 1.3 Preview and apply preferred-leader election for a set of partitions, using `ElectionType.PREFERRED` only. The preview names partitions that would change and partitions already on their first replica. The etag covers leaders and replica sets. A mismatch refuses the apply and sends no election. There is no unclean operation. Verify with a driver test for a mixed set and a stale leader.
- [x] 1.4 Preview and apply a replica reassignment. The preview shows current and new replicas, both replication factors, bytes to copy as an estimate, the throttle, and a duration estimate. Refuse a factor below `min.insync.replicas` before any alter call, naming both numbers. The etag includes replicas, ISR, leader and an in-progress reassignment; an overlap is stale. List in-progress reassignments and cancel one through its own preview. Verify with a driver test for an add, a below-min-ISR refusal, an overlapping move, and a cancel.
- [x] 1.5 Remember the throttle values from before the apply, and expose them on the operation result so the caller can restore them. The result distinguishes keys this operation added from values that were already set. Verify with a driver test that a pre-existing rate is returned unchanged and a rate this operation introduced is marked as added.
- [x] 1.6 Run the new tests on the ZooKeeper 2.8 image, on KRaft 3.9, and on a current 4.x image. Verify the 2.8 leg declares tiered offsets unavailable with a reason, and the newer legs read them. Verify election and reassignment pass on 2.8.
      The move and the already-preferred election now run on the 2.8 and 3.9 legs as well as the current image.

## 2. OpenBroker release

- [x] 2.1 Merge the OpenBroker PR on green CI. Ask a maintainer to run the Release workflow for the version that contains this work, and verify that version resolves from Maven Central. Do not start this release while the previous change's release is still unpublished.
      Released and published; `openbroker-kafka` resolves from Maven Central.
