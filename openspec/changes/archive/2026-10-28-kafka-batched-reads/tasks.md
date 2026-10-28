## 1. Batch the reads
- [x] 1.1 One round for the partitions of all topics of a page of subscriptions. Check: `KafkaBatchedReadsIT`
- [x] 1.2 A read of several queries is one call whose queries share the group, topic and offset rounds. Check: `KafkaBatchedReadsIT` counts Admin calls for 2 and 12 queries
- [x] 1.3 Poms to `0.6.1-SNAPSHOT`, previous version `0.6.0`. Check: build
