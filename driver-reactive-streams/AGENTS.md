# AGENTS.md - driver-reactive-streams

Reactive Streams driver implementing the [Reactive Streams specification](https://www.reactive-streams.org/).

**Depends on:** `bson`, `driver-core`

- Work here if: modifying the Publisher-based async API
- Do not: block in any code path, break backpressure contracts

## Key Packages

- `com.mongodb.reactivestreams.client` — Publisher-based API (`MongoClient`, `MongoDatabase`, `MongoCollection`)

## Build & Test

```bash
./gradlew :driver-reactive-streams:test
./gradlew :driver-reactive-streams:check
```

## Notes

- All operations return `Publisher<T>` — never block
- `driver-kotlin-coroutine` and `driver-scala` both build on top of this module
