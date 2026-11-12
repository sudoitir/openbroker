## Why

Topic create, delete and partition increase, and deletion of an empty group, need a preview that says what will happen before anything changes.

## What Changes

- Create and partition-increase previews validate with the broker and apply nothing.
- A delete preview names groups that still have offsets and estimates the records.
- Internal names, a shrink and a replica assignment are refused before a create or delete is sent.
- Group delete no longer removes members.

## Capabilities

### Modified Capabilities

- `broker-sdk-kafka`: Honest previews for topic create, delete and partition increase, and a group delete that never removes members.
