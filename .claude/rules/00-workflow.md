# Rule: how work happens here

- Every change that alters behaviour or the API goes through OpenSpec (`openspec/changes/<name>/`):
  propose, apply, archive, with no pause for review.
- One git worktree per change; `main` is never pushed to directly. A PR merges when CI is green
  (build, unit tests, TCK on the oldest and newest supported broker of each driver, japicmp). Sonar
  analyses every PR but is advisory: it does not wait for its quality gate and is not part of `ci-ok`.
- A significant decision gets an ADR in `docs/adr/`; an accepted ADR is superseded, never edited.
- Library and API facts come from `npx ctx7@latest`, never memory.
- A driver gap is fixed in the SDK for every driver, never worked around in one driver.
