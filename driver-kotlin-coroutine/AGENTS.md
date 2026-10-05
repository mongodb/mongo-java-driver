# AGENTS.md - driver-kotlin-coroutine

Kotlin Coroutines driver providing `suspend` function-based async API.

**Depends on:** `bson`, `driver-reactive-streams`, `bson-kotlin`

- Work here if: modifying the Kotlin coroutine-based driver API
- Do not: block in any code path — all suspend functions wrap `driver-reactive-streams`

## Key Packages

- `com.mongodb.kotlin.client.coroutine` — Coroutine-based driver API (`MongoClient`, `MongoDatabase`, `MongoCollection`)

## Build & Test

```bash
./gradlew :driver-kotlin-coroutine:test
./gradlew :driver-kotlin-coroutine:check
```
