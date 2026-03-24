# AGENTS.md - MongoDB Java Driver

All changes require human review.
Breaking changes to public API require a major version bump — always warn if binary compatibility is affected.
Also consult `.AGENTS.md` / `~/.AGENTS.md` if present for local agent settings.

See [`.agents/skills/project-guide`](.agents/skills/project-guide/SKILL.md) for module structure and dependency graph.
Each module has its own `AGENTS.md`.

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

```bash
./gradlew check                        # Full validation (format + static checks + tests)
./gradlew :driver-core:test            # Single module tests
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"
```

## Style

`check` runs `spotlessApply` automatically — formatting is enforced.
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

## Do Not Modify Without Human Approval

- Wire protocol / authentication handshakes (`com.mongodb.internal.connection`)
- Connection pool core code (`com.mongodb.internal.connection.pool`)
- Security-critical encryption code / JNA bindings (`mongodb-crypt`)
- Public API contracts (breaking changes need major version bump)
- BSON specification compliance
- Spec test data submodule (`testing/resources/specifications/`)
- Release/versioning scripts, `.evergreen/` config, credentials/secrets

See [`.agents/skills/evergreen`](.agents/skills/evergreen/SKILL.md) for CI validation and patch builds.

## Before Submitting

```bash
./gradlew spotlessApply doc check scalaCheck   # formatting + Docs + static checks + all tests
```
