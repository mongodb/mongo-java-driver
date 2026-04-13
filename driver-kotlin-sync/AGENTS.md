# AGENTS.md - driver-kotlin-sync

Kotlin synchronous (blocking) driver — idiomatic Kotlin wrapper over `driver-sync`.

**Depends on:** `bson`, `driver-sync`, `bson-kotlin`

- Work here if: modifying the Kotlin blocking API surface
- Do not: add internal connection/protocol code (that belongs in `driver-core`)

## Key Packages

- `com.mongodb.kotlin.client` — Blocking sync API (`MongoClient`, `MongoDatabase`, `MongoCollection`)

## Build & Test

```bash
./gradlew :driver-kotlin-sync:test
./gradlew :driver-kotlin-sync:check
```
