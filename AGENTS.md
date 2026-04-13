# AGENTS.md - MongoDB Java Driver

All changes require human review.
Breaking changes to public API require a major version bump — always warn if binary compatibility is affected.
Also consult `.AGENTS.md` / `~/.AGENTS.md` if present for local agent settings.

See [`.agents/skills/project-guide`](.agents/skills/project-guide/SKILL.md) for module structure and dependency graph.
Each module has its own `AGENTS.md`.

## Git

The default branch is `main` (not `master`). Always use `main` when comparing, diffing, or creating branches.

- **Branch naming:** `JAVA-XXXX` matching the Jira ticket (e.g., `JAVA-6143`)
- **Commits:** Keep commits logical and reviewable — each commit should be a coherent unit of change
- **TODO comments:** Must reference a Jira ticket if the work belongs to a different ticket:
  `// TODO JAVA-XXXX reasoning for the todo`

## Core Rules

- Read before modifying.
  Understand existing code and patterns first.
- Minimal changes only.
  No drive-by refactoring or unrelated changes.
- Preserve existing comments.
  Only remove if provably incorrect.
- No rewrites without explicit permission.
- When stuck or uncertain: stop, explain, propose alternatives, ask.

## Build

Gradle with Kotlin DSL. Build JDK: 17+. Source baseline: Java 8. Versions in `gradle/libs.versions.toml`.

**Java 8 language constraint:** Most modules target Java 8. Do not use features from Java 9+ (`var`,
records, text blocks, sealed classes, `Stream.toList()`, switch expressions, pattern matching, etc.)
unless the module's `build.gradle.kts` explicitly sets a higher `sourceCompatibility`.

**Kotlin language constraint:** Kotlin modules use Kotlin 1.8 with JVM target 1.8. All Kotlin modules
enforce `explicitApi()` — all public declarations must have explicit visibility modifiers and types.
Do not use Kotlin language features or standard library APIs introduced after 1.8.

```bash
./gradlew check                        # Full validation (format + static checks + tests)
./gradlew :driver-core:test            # Single module tests
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"
```

## Style

`check` runs `spotlessCheck` to verify formatting — run `./gradlew spotlessApply` to auto-format when needed.
Do not reformat outside your changes.
See [`.agents/skills/style-reference`](.agents/skills/style-reference/SKILL.md) for full rules.

- No `System.out.println` / `System.err.println` — use SLF4J
- No `e.printStackTrace()` — use proper error handling
- Copyright header required: `Copyright 2008-present MongoDB, Inc.`
- Every public package must have a `package-info.java`

## Testing

- Every code change must include tests.
  Do not reduce coverage.
- See [`.agents/skills/testing-guide`](.agents/skills/testing-guide/SKILL.md) for framework details and running specific
  tests.
- See [`.agents/skills/spec-tests`](.agents/skills/spec-tests/SKILL.md) for MongoDB specification test conventions.

## API

All `com.mongodb.internal.*` / `org.bson.internal.*` is private API — never expose in public APIs.
See [`.agents/skills/api-design`](.agents/skills/api-design/SKILL.md) for stability annotations and design principles.

**Java nullability:** Use `com.mongodb.lang.Nullable` / `NonNull` / `NonNullApi` annotations.
Packages with `@NonNullApi` in `package-info.java` are non-null by default — only annotate exceptions
with `@Nullable`. New packages must include `@NonNullApi`. Older packages without it are nullable by
default and require explicit `@NonNull` where needed.

**Thread safety:** Use `com.mongodb.annotations`; `@ThreadSafe`, `@NotThreadSafe`, or `@Immutable`.
All public API classes must be thread-safe unless annotated otherwise. Concurrent code — particularly
in async `driver-core` paths — must use locks (via `Locks.withLock()`), `volatile` fields, or
`java.util.concurrent` atomics; never rely on external synchronization.

## Do Not Modify Without Human Approval

- Wire protocol / authentication handshakes (`com.mongodb.internal.connection`)
- Connection pool core code (`com.mongodb.internal.connection.pool`)
- Security-critical encryption code / JNA bindings (`mongodb-crypt`)
- Public API contracts (breaking changes need major version bump)
- BSON specification compliance
- Spec test data submodule (`testing/resources/specifications/`)
- Release/versioning scripts, `.evergreen/` config, credentials/secrets

See [`.agents/skills/evergreen`](.agents/skills/evergreen/SKILL.md) for CI validation and patch builds.

## Dependencies

- Never add dependencies without justification
- All dependency versions are managed in `gradle/libs.versions.toml` — never declare versions inline in `build.gradle.kts`

## Before Submitting

```bash
./gradlew spotlessApply doc check scalaCheck   # formatting + Docs + static checks + all tests
```
