# AGENTS.md - testing

Shared test resources and MongoDB specification test data.

## Notes

- `resources/specifications/` — Git submodule containing MongoDB specification test data (CRUD, SDAM, auth, CSFLE,
  retryable ops, etc.)
- `resources/logback-test.xml` — Shared logback configuration for tests
- **Do not modify spec test data** — managed upstream.
  Update via `git submodule update`.
