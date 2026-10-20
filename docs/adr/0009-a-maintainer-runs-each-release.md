# ADR-0009: A maintainer runs each release

- **Status**: accepted; amends [ADR-0008](0008-semver-0x-with-japicmp-and-incubating-types.md)
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

ADR-0008 made a pushed `vX.Y.Z` tag the release: CI built, signed and published every module to Maven
Central as soon as the tag arrived. Maven Central limits how much a namespace publishes, and the
`io.github.sudoitir` namespace is shared with other projects, so every upload counts against one allowance.
A tag pushed by mistake, or a tag pushed again after a failed run, published without anyone deciding to.

## Decision

A release is a run of the `Release` workflow, started by a maintainer on `main` with the version
(`Actions → Release → Run workflow`, or `gh workflow run release.yml -f version=X.Y.Z`).

- The run refuses any ref but `main`, a version that is not `X.Y.Z`, and a version whose tag exists.
- It publishes every module at that version, attests the jars, then tags the commit `vX.Y.Z` and creates
  the GitHub release. The tag records a release; it no longer causes one.
- Only people with write access to the repository can start it. Runs do not overlap.

## Consequences

- Nothing reaches Central unless a maintainer asks for it, so merges cost no part of the allowance.
- A release needs one deliberate step after the merge. CI on the pull request remains the verification;
  the release run does not test again.
- A failed run leaves no tag, so the same version can be run again once the cause is fixed.

## Alternatives considered

- **Keep the tag trigger.** Already deliberate, but a stray or repeated tag still publishes.
- **A tag trigger behind an environment that needs an approval.** Two steps for one decision, and the
  tag exists even when the approval is refused.
