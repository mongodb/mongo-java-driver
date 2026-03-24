---
name: project-guide
description: Project structure, dependency graph, and guide for finding the right project to work in. Use when starting a task and unsure which project owns the relevant code, or when you need to understand cross-project dependencies.
---
# Project Guide

## Project Structure

| Project | Purpose |
| --- | --- |
| `bson` | BSON library (core serialization) |
| `bson-kotlin` | Kotlin BSON extensions |
| `bson-kotlinx` | Kotlin serialization BSON codec |
| `bson-record-codec` | Java record codec support |
| `bson-scala` | Scala BSON extensions |
| `driver-core` | Core driver internals (connections, protocol, operations) |
| `driver-sync` | Synchronous Java driver |
| `driver-legacy` | Legacy MongoDB Java driver API |
| `driver-reactive-streams` | Reactive Streams driver |
| `driver-kotlin-coroutine` | Kotlin Coroutines driver |
| `driver-kotlin-extensions` | Kotlin driver extensions |
| `driver-kotlin-sync` | Kotlin synchronous driver |
| `driver-scala` | Scala driver |
| `mongodb-crypt` | Client-side field-level encryption |
| `bom` | Bill of Materials for dependency management |
| `testing` | Shared test resources and MongoDB specifications |
| `buildSrc` | Gradle convention plugins and build infrastructure |
| `driver-benchmarks` | JMH and custom performance benchmarks (not published) |
| `driver-lambda` | AWS Lambda test application (not published) |
| `graalvm-native-image-app` | GraalVM Native Image compatibility testing (not published) |

## Dependency Graph (simplified)

```
bson
 ├── bson-kotlin
 ├── bson-kotlinx
 ├── bson-record-codec
 ├── bson-scala
 └── driver-core
      ├── driver-sync
      │    ├── driver-legacy
      │    ├── driver-kotlin-sync
      │    └── driver-lambda
      ├── driver-reactive-streams
      │    ├── driver-kotlin-coroutine
      │    └── driver-scala
      └── driver-kotlin-extensions
```

## Finding the Right Module

- **BSON serialization/codecs:** `bson` (or `bson-kotlin`/`bson-kotlinx`/`bson-scala` for language-specific)
- **Query builders, filters, aggregates:** `driver-core` (`com.mongodb.client.model`)
- **Sync Java API:** `driver-sync`
- **Reactive API:** `driver-reactive-streams`
- **Kotlin coroutines:** `driver-kotlin-coroutine`
- **Kotlin DSL builders:** `driver-kotlin-extensions`
- **Scala driver:** `driver-scala`
- **Connection, protocol, auth internals:** `driver-core` (`com.mongodb.internal.*`)
- **Build plugins, formatting, test infra:** `buildSrc`

Each module has its own `AGENTS.md` with module-specific packages, patterns, and notes.
