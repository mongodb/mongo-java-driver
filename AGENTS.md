# AGENTS.md - MongoDB Java Driver

All changes require human review.
Breaking changes to public API require a major version bump — always warn if binary compatibility is affected.
Also consult `.AGENTS.md` / `~/.AGENTS.md` if present for local agent settings.

**Architecture:** Multi-module Gradle project: `bson` → `driver-core` → `driver-sync`/`driver-reactive-streams` → language wrappers (`driver-kotlin-sync`, `driver-kotlin-coroutine`, `driver-scala`).

See [`.agents/references/project-guide`](.agents/references/project-guide.md) for module structure and dependency graph.
Each module has its own `AGENTS.md`.

## Git

The default branch is `main` (not `master`). Always use `main` when comparing, diffing, or creating branches.

- **Branch naming:** `JAVA-XXXX` matching the Jira ticket (e.g., `JAVA-6143`)
- **Commits:** Keep commits logical and reviewable — each commit should be a coherent unit of change
- **TODO comments:** Must reference a Jira ticket if the work belongs to a different ticket:
  `// TODO JAVA-XXXX reasoning for the todo`

## Core Rules

- Read before modifying — understand existing code and patterns first
- Minimal changes only — no drive-by refactoring or unrelated changes
- Preserve existing comments — only remove if provably incorrect
- No rewrites without explicit permission
- When stuck or uncertain: stop, explain, propose alternatives, ask

## Build

Gradle with Kotlin DSL. Build JDK: 17+. Source baseline: Java 8. Versions in `gradle/libs.versions.toml`.

- **Java 8 baseline:** no Java 9+ language features unless the module raises `sourceCompatibility`.
- **Kotlin 1.8** with JVM target 1.8; all Kotlin modules enforce `explicitApi()`.

Details for both are in [`.agents/references/style-reference`](.agents/references/style-reference.md).

```bash
./gradlew check                        # Full validation (format + static checks + tests)
./gradlew :driver-core:test            # Single module tests
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"
```

## Style

`check` depends on `spotlessApply` to auto-fix formatting — run `./gradlew spotlessApply` independently when needed.
Do not reformat outside your changes.
See [`.agents/references/style-reference`](.agents/references/style-reference.md) for prohibited
patterns, required headers, and full formatting rules.

## Testing

- Every code change must include tests.
  Do not reduce coverage.
- Integration tests require a running MongoDB instance (see the test URI in Build section) —
  do not attempt to run `integrationTest` without a configured server.
- See [`.agents/references/testing-guide`](.agents/references/testing-guide.md) for framework details and running specific
  tests.
- See [`.agents/references/spec-tests`](.agents/references/spec-tests.md) for MongoDB specification test conventions.

## API

All `com.mongodb.internal.*` / `org.bson.internal.*` is private API — never expose in public APIs.

**Nullability:** use `com.mongodb.lang` annotations; new packages must declare `@NonNullApi` in
`package-info.java`.

**Thread safety:** use `com.mongodb.annotations` (`@ThreadSafe` / `@NotThreadSafe` / `@Immutable`);
public API classes must be thread-safe unless annotated otherwise.

See [`.agents/references/api-design`](.agents/references/api-design.md) for stability annotations,
design principles, and the full nullability and thread safety conventions.

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
./gradlew spotlessApply docs check scalaCheck   # formatting + docs + static checks + all tests
```
