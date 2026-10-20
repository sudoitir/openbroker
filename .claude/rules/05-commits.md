# Rule: commit messages

Conventional Commits 1.0.0, imperative mood, no trailing period: `type(scope)!: description`.
The scope names a module or area (`api`, `artemis`, `tck`, `build`).

- `feat`, `fix`, `perf`, `security`, `remove` describe what an adopter sees; write the body for them.
- A breaking API change carries `!` and a `BREAKING CHANGE:` paragraph with the migration.
- `docs`, `test`, `chore`, `ci`, `build`, `refactor` for internal work.
- Never name a product that uses OpenBroker, or a private repository.
