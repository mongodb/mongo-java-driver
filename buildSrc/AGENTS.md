# AGENTS.md - buildSrc

Gradle build infrastructure providing convention plugins and shared configuration for
all modules.

## Key Directories

- `src/main/kotlin/conventions/` — Convention plugins applied by modules (formatting,
  testing, publishing, static analysis)
- `src/main/kotlin/project/` — Base project plugins for Java, Kotlin, and Scala modules
- `src/main/kotlin/ProjectExtensions.kt` — Shared Gradle extension utilities

## Convention Plugins

| Plugin | Purpose |
| --- | --- |
| `spotless` | Code formatting (Java: Palantir; Kotlin: ktfmt dropbox, max 120; Scala: scalafmt) |
| `codenarc` | Groovy static analysis |
| `detekt` | Kotlin static analysis |
| `spotbugs` | Java bug detection |
| `bnd` | OSGi bundle metadata |
| `dokka` | Kotlin documentation generation |
| `javadoc` | Java documentation generation |
| `scaladoc` | Scala documentation generation |
| `publishing` | Maven Central publishing configuration |
| `git-version` | Version derivation from git tags |
| `optional` | Optional dependency support (`optionalImplementation`) |
| `testing-base` | Shared test configuration |
| `testing-integration` | Integration test source set and tasks |
| `testing-junit` | JUnit 5 test setup |
| `testing-junit-vintage` | JUnit 4 compatibility |
| `testing-spock` | Spock framework setup |
| `testing-spock-exclude-slow` | Spock with slow test exclusion |
| `testing-mockito` | Mockito setup |
| `test-artifacts` | Test artifact sharing between modules |
| `test-artifacts-runtime-dependencies` | Runtime dependency sharing for tests |
| `test-include-optionals` | Include optional dependencies in tests |

## Project Plugins

| Plugin | Purpose |
| --- | --- |
| `project/base` | Common settings for all modules |
| `project/java` | Java module conventions (source compatibility, checkstyle) |
| `project/kotlin` | Kotlin module conventions |
| `project/scala` | Scala module conventions (multi-version support) |

## Notes

- Formatting: ktfmt dropbox style, max width 120 (for buildSrc’s own code)
- `check` depends on `spotlessCheck` in buildSrc itself
- Java toolchain is set to Java 17

## Keeping AGENTS.md and Skills in Sync

When modifying buildSrc, you **must** update the relevant AGENTS.md files and skills (in
`.agents/skills/`) if your changes affect:

- **Formatting conventions** (e.g., changes to `spotless.gradle.kts`) — update the root
  `AGENTS.md` “Formatting” section, the `style-reference` skill, and any module
  AGENTS.md files that reference formatting rules
- **Convention plugins added or removed** — update this file’s plugin table and the root
  `AGENTS.md` if the change affects build commands or developer workflow
- **Testing conventions** (e.g., changes to `testing-*.gradle.kts`) — update the root
  `AGENTS.md` “Testing” section, the `testing-guide` skill, and affected module
  AGENTS.md files
- **Project plugins added or removed** — update this file’s project plugin table
- **Build commands or task names changed** — update the root `AGENTS.md` “Build” and
  “Before Submitting” sections, the `evergreen` skill, and any module “Before
  Submitting” sections
- **CI/CD or publishing changes** — update the `evergreen` skill
