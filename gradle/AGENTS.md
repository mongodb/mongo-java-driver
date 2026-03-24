# AGENTS.md - gradle

Gradle configuration and infrastructure for the build system.

## Key Files

- `libs.versions.toml` — Central version catalog defining all dependency versions, library coordinates, bundles, and plugin versions
- `wrapper/` — Gradle wrapper JAR and properties (do not modify manually — use `./gradlew wrapper --gradle-version=<version>`)
- `scala/lib/` — Bundled Scala Ant JAR for Scala compilation support

## Notes

- All dependency versions are managed in `libs.versions.toml` — never declare versions inline in `build.gradle.kts` files
- **Never add dependencies without justification.** Dependencies are liabilities.
- **Use dependencies for:** Security/crypto, complex protocols, battle-tested algorithms.
- **Write it yourself for:** fewer than 100 LOC of straightforward code, project-specific logic, performance-critical paths.
- The version catalog defines `[versions]`, `[libraries]`, `[bundles]`, and `[plugins]` sections
- Scala test libraries are declared per Scala version (v3, v2-v13, v2-v12, v2-v11)
- When adding or updating dependencies, modify `libs.versions.toml` and reference via `libs.<alias>` in build scripts
