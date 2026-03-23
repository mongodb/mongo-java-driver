# CLAUDE.md - driver-scala

Scala async driver providing Observable-based API wrapping `driver-reactive-streams`.

**Depends on:** `bson-scala`, `driver-reactive-streams`

**Supported Scala versions:** 2.11, 2.12, 2.13, 3 (configured in root `gradle.properties`). Default: 2.13.

See [README.md](./README.md) for full details on directory layout, formatting, and testing commands.

## Key Packages

- `org.mongodb.scala` — Scala async driver (`MongoClient`, `MongoDatabase`, `MongoCollection`, Observable-based)
- `org.mongodb.scala.model` — Scala wrappers around filter/update builders

## Before Submitting

```bash
./gradlew spotlessApply                                             # Fix formatting
./gradlew :driver-scala:scalaCheck                                  # Static checks + tests (default Scala version)
./gradlew :driver-scala:scalaCheck -PscalaVersion=<version>         # Test specific Scala version
```
