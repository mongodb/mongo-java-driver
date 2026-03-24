# AGENTS.md - driver-kotlin-extensions

Kotlin extensions providing type-safe DSLs for query and update construction.

**Depends on:** `driver-core`, optionally `driver-kotlin-sync`,
`driver-kotlin-coroutine`

## Key Packages

- `com.mongodb.kotlin.client.model` — Type-safe DSL builders (`Filters`, `Updates`,
  `Aggregates`, `Sorts`, `Projections`, `Indexes`)
- `com.mongodb.kotlin.client.property` — `KPropertyPath` for property-based queries

## Notes

- Works with both `driver-kotlin-sync` and `driver-kotlin-coroutine`
