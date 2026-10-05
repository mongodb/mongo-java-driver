# AGENTS.md - driver-kotlin-extensions

Kotlin extensions providing type-safe DSLs for query and update construction.

**Depends on:** `driver-core`, optionally `driver-kotlin-sync`, `driver-kotlin-coroutine`

- Work here if: adding or modifying Kotlin type-safe DSL builders for `Filters`, `Updates`, `Aggregates`, etc.
- Works with both `driver-kotlin-sync` and `driver-kotlin-coroutine`

## Key Packages

- `com.mongodb.kotlin.client.model` — Type-safe DSL builders
- `com.mongodb.kotlin.client.property` — `KPropertyPath` for property-based queries

## Build & Test

```bash
./gradlew :driver-kotlin-extensions:test
./gradlew :driver-kotlin-extensions:check
```
