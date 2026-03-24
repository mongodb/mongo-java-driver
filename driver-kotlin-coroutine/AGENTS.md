# AGENTS.md - driver-kotlin-coroutine

Kotlin Coroutines driver providing `suspend` function-based async API.

**Depends on:** `bson`, `driver-reactive-streams`, `bson-kotlin`

## Key Packages

- `com.mongodb.kotlin.client.coroutine` — Coroutine-based driver API (`MongoClient`, `MongoDatabase`, `MongoCollection`)

## Notes

- Formatting: ktfmt dropbox style, max width 120
- Suspend functions wrapping `driver-reactive-streams` — never block
- Built on `kotlinx-coroutines`
