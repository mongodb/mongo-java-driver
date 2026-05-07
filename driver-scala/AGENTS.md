# AGENTS.md - driver-scala

Scala async driver providing Observable-based API wrapping `driver-reactive-streams`.

**Depends on:** `bson-scala`, `driver-reactive-streams`

**Supported Scala versions:** 2.11, 2.12, 2.13, 3 (default: 2.13, configured in root `gradle.properties`).

- Work here if: modifying the Scala Observable-based driver API or Scala model wrappers
- Do not: block in any code path

## Key Packages

- `org.mongodb.scala` — Scala async driver (`MongoClient`, `MongoDatabase`, `MongoCollection`)
- `org.mongodb.scala.model` — Scala wrappers around filter/update builders

## Build & Test

```bash
./gradlew :driver-scala:test
./gradlew :driver-scala:scalaCheck                          # Static checks + tests (default Scala version)
./gradlew :driver-scala:scalaCheck -PscalaVersion=<version> # Test specific Scala version
```

See [README.md](./README.md) for directory layout details.
