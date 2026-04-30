# AGENTS.md - bson-scala

Scala extensions for the BSON library, providing Scala-idiomatic wrappers and macro-based codecs.

**Depends on:** `bson`

**Supported Scala versions:** 2.11, 2.12, 2.13, 3 (default: 2.13, configured in root `gradle.properties`).

- Work here if: modifying Scala BSON wrappers, macro codecs, or Scala collection support

## Key Packages

- `org.mongodb.scala.bson` — Core Scala BSON wrappers
- `org.mongodb.scala.bson.codecs` — Macro-based codecs (Scala 2/3)

## Build & Test

```bash
./gradlew :bson-scala:test
./gradlew :bson-scala:scalaCheck                          # Static checks + tests (default Scala version)
./gradlew :bson-scala:scalaCheck -PscalaVersion=<version> # Test specific Scala version
```

See [README.md](./README.md) for directory layout details.
