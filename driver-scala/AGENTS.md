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

## Notes

- **Mirror new public Java types in Scala.** Adding a public type or enum under a
  wrapped package (`com.mongodb`, `com.mongodb.client.model` and its subpackages,
  `com.mongodb.connection`, `com.mongodb.client.result`, …) requires a matching wrapper
  in the corresponding `org.mongodb.scala.*` package — this is enforced by
  `ApiAliasAndCompanionSpec` (the build fails if a mirror is missing):
  - Add a `type` alias in the package's `package.scala`, copying the Java class's
    stability annotations (`@Sealed`, `@Beta(Reason.…)`).
  - **Keep `@Beta` annotations in sync with their Java counterpart.** The Scala alias
    (and any companion `object`) must carry `@Beta` if and only if the Java class does,
    with the same `Reason` value(s). When a Java type is promoted to stable (its
    `@Beta` removed) or its `Reason` changes, update the Scala side to match — a
    stable Java class (no `@Beta`) must not carry a stale `@Beta` on its Scala alias.
  - **Every wrapped public enum needs a companion `object`** re-exposing each constant
    as a `val` (see `ReturnDocument.scala`, `CollationStrength.scala`); a `type` alias
    alone does not bring the enum constants into term scope, so `MyEnum.CONSTANT` would
    not resolve for Scala users.
  - For a type with factory methods, add a companion `object`. Follow the surrounding
    package's convention: `model` uses an inline `object X { def apply(): X = new … }`
    in `package.scala` for constructor-based options, whereas `model/search` uses a
    dedicated-file `object` exposing each factory under its Java name (e.g.
    `vectorSearchNestedOptions()`), never `apply()`.
