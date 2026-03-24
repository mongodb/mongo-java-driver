# AGENTS.md - driver-reactive-streams

Reactive Streams driver implementing the
[Reactive Streams specification](https://www.reactive-streams.org/).

**Depends on:** `bson`, `driver-core`

## Key Packages

- `com.mongodb.reactivestreams.client` — Publisher-based API (`MongoClient`,
  `MongoDatabase`, `MongoCollection`)

## Notes

- JUnit 5 + Spock (Groovy) + Project Reactor (test utilities)

## Key Patterns

- All operations return `Publisher<T>` — never block
- Respect backpressure in all implementations
- `driver-kotlin-coroutine` and `driver-scala` both build on top of this module
