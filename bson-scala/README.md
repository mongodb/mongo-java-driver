# Scala Bson library

The `bson-scala` project provides Scala-idiomatic wrappers for the Java Bson library.
It currently supports: **Scala 2.11**, **Scala 2.12**, **Scala 2.13**, and **Scala 3**.

## Scala Versions

Supported Scala versions and their exact version numbers are defined in [`gradle.properties`](gradle.properties):

- `supportedScalaVersions` — the list of supported Scala versions
- `defaultScalaVersion` — the version used when no `-PscalaVersion` flag is provided (currently `2.13`)

## Build Configuration

The Scala source set configuration, compiler options, and dependency wiring are all handled in [`buildSrc/src/main/kotlin/project/scala.gradle.kts`](buildSrc/src/main/kotlin/project/scala.gradle.kts).

## Library Dependencies

Scala library and test dependencies for each version are defined in [`gradle/libs.versions.toml`](gradle/libs.versions.toml). Look for entries prefixed with `scala-` in the `[versions]`, `[libraries]`, and `[bundles]` sections.

## Directory Layout

Source code is organized into version-specific directories.
Shared code goes in the common `scala` directory, while version-specific code goes in the appropriate directory:

```
src/main/
├── scala/          # Shared code (all Scala versions)
├── scala-2/        # Scala 2 only (2.11, 2.12 and 2.13)
├── scala-2.13/     # Scala 2.13 only
├── scala-2.13-/    # Scala 2.12 & 2.11
├── scala-3/        # Scala 3 only
```

Test code also supports the same directory structure.
The source sets for each Scala version are configured in [`buildSrc/src/main/kotlin/project/scala.gradle.kts`](buildSrc/src/main/kotlin/project/scala.gradle.kts).
When adding new code, place it in the most general directory that applies. Only use a version-specific directory when the code requires syntax or APIs unique to that version.

## Code Formatting (Spotless)

Spotless defaults to **Scala 2.13** formatting rules. This means code in shared directories (`scala/`, `scala-2/`) is formatted with the 2.13 scalafmt configuration.

For **Scala 3 specific code**, the `bson-scala/build.gradle.kts` shows how to configure Spotless to use a Scala 3 scalafmt config. It targets only files in `**/scala-3/**` and uses a separate `config/scala/scalafmt-3.conf`:

```kotlin
if (scalaVersion.equals("3")) {
    spotless {
        scala {
            clearSteps()
            target("**/scala-3/**")
            scalafmt("3.10.7").configFile(rootProject.file("config/scala/scalafmt-3.conf"))
        }
    }
}
```

Use this pattern in other `build.gradle.kts` files if they also contain Scala 3 specific code.

## Testing

By default, tests run against Scala 2.13. To test against a specific Scala version, pass the `-PscalaVersion` flag:

```bash
# Test bson-scala with Scala 3
./gradlew :bson-scala:scalaCheck -PscalaVersion=3

# Test bson-scala with Scala 2.12
./gradlew :bson-scala:scalaCheck -PscalaVersion=2.12

# Test bson-scala with the default (2.13)
./gradlew :bson-scala:scalaCheck
```
