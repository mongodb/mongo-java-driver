# CLAUDE.md - MongoDB Java Driver

Guidelines for AI agents working on the MongoDB Java Driver codebase.

## Project Overview

This is the official MongoDB JVM driver monorepo, providing Java, Kotlin, and Scala
drivers for MongoDB. The driver implements the
[MongoDB Driver Specifications](https://github.com/mongodb/specifications)
and follows semantic versioning.

### Module Structure

| Module                     | Purpose                                                   |
|----------------------------|-----------------------------------------------------------|
| `bson`                     | BSON library (core serialization)                         |
| `bson-kotlin`              | Kotlin BSON extensions                                    |
| `bson-kotlinx`             | Kotlin serialization BSON codec                           |
| `bson-record-codec`        | Java record codec support                                 |
| `bson-scala`               | Scala BSON extensions                                     |
| `driver-core`              | Core driver internals (connections, protocol, operations) |
| `driver-sync`              | Synchronous Java driver                                   |
| `driver-legacy`            | Legacy MongoDB Java driver API                            |
| `driver-reactive-streams`  | Reactive Streams driver                                   |
| `driver-kotlin-coroutine`  | Kotlin Coroutines driver                                  |
| `driver-kotlin-extensions` | Kotlin driver extensions                                  |
| `driver-kotlin-sync`       | Kotlin synchronous driver                                 |
| `driver-scala`             | Scala driver                                              |
| `mongodb-crypt`            | Client-side field-level encryption                        |
| `testing`                  | Shared test resources and MongoDB specifications          |

### Internal API Convention

All code in `com.mongodb.internal.*` and `org.bson.internal.*` is private API.
It can change at any time without notice. Never expose internal types in
public APIs, and never advise users to depend on them.

### API Stability Annotations

- `@Alpha` - Early development, may be removed. Not for production use.
- `@Beta` - Subject to change or removal. Libraries should not depend on these.
- `@Evolving` - May add abstract methods in future releases. Safe to use, but implementing/extending bears upgrade risk.
- `@Sealed` - Must not be extended or implemented by consumers. Safe to use, but not to subclass.
- `@Deprecated` - Supported until next major release but should be migrated away from.

## General

- Follow existing conventions. Keep it simple.
- When stuck: stop, explain the problem, propose alternatives, ask for guidance.
- When uncertain: ask rather than assume. Present options with trade-offs.

## Build System

- **Build tool:** Gradle with Kotlin DSL
- **Build JDK:** Java 17+
- **Source compatibility (baseline):** Java 8 for most modules / main driver artifacts
- **Version catalog:** `gradle/libs.versions.toml`
- **Convention plugins:** `buildSrc/src/main/kotlin/conventions/`

### Essential Build Commands

```bash
# Full validation (static checks + unit tests + integration tests)
./gradlew check

# Integration tests (requires running MongoDB)
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"

# Single module tests
./gradlew :driver-core:test

# Format check only
./gradlew spotlessCheck

# Auto-fix formatting
./gradlew spotlessApply

# Test with alternative JDK
./gradlew test -PjavaVersion=11
```

## Code Style and Formatting

Formatting is enforced automatically because the Gradle `check` task depends on
`./gradlew spotlessApply`, which formats sources. To run a check without modifying
files, invoke `./gradlew spotlessCheck` manually. Do not manually reformat files
outside the scope of your changes.

### Style Rules

- **Max line length:** 140 characters
- **Indentation:** 4 spaces (no tabs)
- **Line endings:** LF (Unix)
- **Charset:** UTF-8
- **Star imports:** Prohibited (AvoidStarImport)
- **Final parameters:** Required (FinalParameters checkstyle rule)
- **Braces:** Required for all control structures (NeedBraces)
- **Else placement:** On its own line (not cuddled)
- **Copyright header:** Every Java / Kotlin / Scala file must contain `Copyright 2008-present MongoDB, Inc.`

### Prohibited Patterns

- `System.out.println` / `System.err.println` — Use SLF4J logging
- `e.printStackTrace()` — Use proper logging/error handling

## Testing

### Frameworks

- **JUnit 5** (Jupiter) - Primary unit test framework
- **Spock** (Groovy) - Legacy, do not add new Spock tests.
- **Mockito** - Mocking
- **ScalaTest** - Scala module testing

### Writing Tests

- **Every code change must include tests.** New code needs new tests; modified code needs updated tests. Do not reduce test coverage.
- Extend existing test patterns in the module you are modifying
- Unit tests should not require a running MongoDB instance
- Test methods should be descriptive:
  prefer `shouldReturnEmptyListWhenNoDocumentsMatch()` over `test1()`,
  use `@DisplayName` for a human readable name
- Clean up test data in `@AfterEach` / `cleanup()` to prevent test pollution

### MongoDB Specifications and Specification Tests

The driver implements the [MongoDB Specifications](https://github.com/mongodb/specifications). Specification test data files live in
`testing/resources/specifications/` — a git submodule for test resources. When modifying driver specification-related behavior: do
not modify existing specification tests unless they test incorrect behavior. Create new tests instead.

## Core Development Principles

### Essential Rules

- **Read before modifying.** Understand existing code and patterns before making changes.
- **Minimal changes only.** No drive-by refactoring.
- **Preserve existing comments.** Only remove comments that are provably incorrect.
- **No unrelated changes.** Do not fix formatting or refactor code outside the scope of the task.
- **No rewrites** without explicit permission.

### Search Before Implementing

Before writing new code, search the codebase for existing implementations:

- Check if a utility method already exists in `com.mongodb.internal.*`
- Check if a similar pattern is established elsewhere in the module
- Reuse existing well-tested infrastructure over creating duplicates

### API Design

- **Deep modules:** Prefer simple interfaces with powerful implementations over shallow wrappers.
- **Information hiding:** Bury complexity behind simple interfaces.
- **Pull complexity downward:** Make the implementer work harder so callers work less.
- **General-purpose over special-case:** Fewer flexible methods over many specialized ones.
- **Define errors out of existence:** Design APIs so errors cannot happen rather than detecting and handling them.

## Dependency Management

Dependencies are managed centrally via `gradle/libs.versions.toml`.

- **Never add dependencies without justification.** Dependencies are liabilities.
- **Use dependencies for:** Security/crypto, complex protocols, battle-tested algorithms.
- **Write it yourself for:** fewer than 100 LOC of straightforward code, project-specific logic, performance-critical paths.
- Optional dependencies (Netty, Snappy, Zstd, AWS SDK, SLF4J) use `optionalImplementation` and must not be required at runtime.

## Safety Rules - Do Not Modify Without Review

- Wire protocol implementation (connection/authentication handshakes)
- Security-critical authentication and encryption code
- Public API contracts (breaking changes require major version bump)
- BSON specification compliance
- Configuration files containing credentials or secrets
- CI/CD pipeline configurations in `.evergreen/`
- Release and publishing scripts

## CI/CD

### Evergreen (MongoDB Internal CI)

Primary CI runs on MongoDB's Evergreen system. Configuration is in `.evergreen/`.

### Before Submitting

Run locally at minimum:

```bash
./gradlew doc check scalaCheck # Generates docs, runs static checks + tests
```
