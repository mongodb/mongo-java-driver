# AGENTS.md - graalvm-native-image-app

GraalVM Native Image test application verifying driver compatibility with native compilation.

**Depends on:** `bson`, `driver-core`, `driver-sync`, `driver-legacy`, `driver-reactive-streams`, `mongodb-crypt`

## Key Packages

- `com.mongodb.internal.graalvm` — Native image test app, GraalVM substitutions, and custom DNS support

## Notes

- Not published — internal testing only
- Requires `-PincludeGraalvm` Gradle property to be included in the build
- Requires GraalVM JDK 21+ installed and detected by Gradle toolchains
- Reachability metadata in `src/main/resources/META-INF/native-image/`
- Non-standard source layout: `src/main` (no `java` subdirectory)
