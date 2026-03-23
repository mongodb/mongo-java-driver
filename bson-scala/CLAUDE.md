# CLAUDE.md - bson-scala

Scala extensions for the BSON library, providing Scala-idiomatic wrappers and macro-based codecs.

**Depends on:** `bson`

**Supported Scala versions:** 2.11, 2.12, 2.13, 3 (configured in root `gradle.properties`). Default: 2.13.

See [README.md](./README.md) for full details on directory layout, formatting, and testing commands.

## Key Packages

- `org.mongodb.scala.bson` — Core Scala BSON wrappers
- `org.mongodb.scala.bson.codecs` — Macro-based codecs (Scala 2/3)
- `org.mongodb.scala.bson.collection` — Immutable/mutable collection support
- `org.mongodb.scala.bson.annotations` — Field annotations

## Before Submitting

```bash
./gradlew spotlessApply                                           # Fix formatting
./gradlew :bson-scala:scalaCheck                                  # Static checks + tests (default Scala version)
./gradlew :bson-scala:scalaCheck -PscalaVersion=<version>         # Test specific Scala version
```
