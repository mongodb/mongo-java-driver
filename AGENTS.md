# AGENTS.md - MongoDB Java Driver

Official MongoDB JVM driver monorepo (Java, Kotlin, Scala).
Implements the
[MongoDB Driver Specifications](https://github.com/mongodb/specifications).
Each module has its own `AGENTS.md` with module-specific guidance.

## Core Rules

- Read before modifying.
  Understand existing code and patterns first.
- Minimal changes only.
  No drive-by refactoring or unrelated changes.
- Preserve existing comments.
  Only remove if provably incorrect.
- No rewrites without explicit permission.
- When stuck or uncertain: stop, explain, propose alternatives, ask.
- All `com.mongodb.internal.*` / `org.bson.internal.*` is private API — never expose in
  public APIs.

## Build

- Gradle with Kotlin DSL. Build JDK: 17+. Source baseline: Java 8.
- Versions: `gradle/libs.versions.toml`. Plugins:
  `buildSrc/src/main/kotlin/conventions/`.

```bash
./gradlew check                        # Full validation (format + static checks + tests)
./gradlew :driver-core:test            # Single module tests
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"
```

## Formatting

`check` runs `spotlessApply` automatically.
Do not reformat outside your changes.

- No `System.out.println` / `System.err.println` — use SLF4J
- No `e.printStackTrace()` — use proper error handling
- Copyright header required: `Copyright 2008-present MongoDB, Inc.`
- Every public package must have a `package-info.java`

## Testing

- Every code change must include tests.
  Do not reduce coverage.
- JUnit 5 primary. Spock is legacy — do not add new Spock tests.
- Unit tests must not require a running MongoDB instance.
- Descriptive method names or `@DisplayName`. Clean up in `@AfterEach`.

## Safety — Do Not Modify Without Review

- Wire protocol / authentication handshakes
- Security-critical encryption code
- Public API contracts (breaking changes need major version bump)
- BSON specification compliance
- Credentials, secrets, `.evergreen/` config, release scripts

## Before Submitting

```bash
./gradlew doc check scalaCheck
```
