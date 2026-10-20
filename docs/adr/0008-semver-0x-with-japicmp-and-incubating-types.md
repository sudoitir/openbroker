# ADR-0008: SemVer `0.x`, checked by japicmp, with incubating types

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

Adopters need to know when an upgrade breaks them. Part of the API (the transport) is expected to
change before the messaging client settles it.

## Decision

- Versions are SemVer; until 1.0 a breaking change raises the minor version. A release is a `vX.Y.Z`
  tag, built, signed and published to Maven Central by CI, with build provenance.
- japicmp compares every module (the Artemis driver's public types included) with the version in
  `openbroker.previous.version`, set explicitly, and fails the build on an undeclared break.
- `@Incubating` (runtime-retained) marks types that may change in any release; japicmp excludes them.
  Today that is the transport.
- `openbroker-bom` aligns the modules; adopters upgrade them together.

## Consequences

- Breaks are deliberate and visible; a consumer pins one version for all modules.
- Incubating types carry a stated risk until they lose the annotation.

## Alternatives considered

- **Comparing against the latest release automatically.** A failed or skipped release would move the
  baseline silently.
