---
name: testing-guide
description: Testing frameworks, conventions, and commands for the MongoDB Java Driver. Use when writing or running tests — covers framework selection per module, test naming conventions, integration test setup, and how to run specific test subsets.
---
# Testing Guide

## Frameworks

| Framework | Usage | Notes |
| --- | --- | --- |
| JUnit 5 (Jupiter) | Primary unit test framework | All new tests |
| Spock (Groovy) | Legacy tests | Do not add new Spock tests |
| Mockito | Mocking | Use `mockito-junit-jupiter` integration |
| ScalaTest | Scala module testing | FlatSpec + ShouldMatchers |
| Project Reactor | Reactive test utilities | `driver-reactive-streams` tests |

## Assertions

- **JUnit Jupiter Assertions** (`org.junit.jupiter.api.Assertions`) is the standard for all new tests
- Hamcrest matchers appear in older tests but should not be used in new code
- AssertJ is in the dependency catalog but is not used — do not introduce it
- Spock tests use Spock's built-in `expect:`/`then:` assertions (no external assertion library)

## Writing Tests

- Every code change must include tests
- Extend existing test patterns in the module you are modifying
- Unit tests must not require a running MongoDB instance
- Descriptive method names: `shouldReturnEmptyListWhenNoDocumentsMatch()` not `test1()`
- Use `@DisplayName` for human-readable names
- Clean up test data in `@AfterEach` / `cleanup()` to prevent pollution

## Running Tests

```bash
# All tests (unit + integration)
./gradlew check

# Single module
./gradlew :driver-core:test

# Integration tests (requires MongoDB)
./gradlew integrationTest -Dorg.mongodb.test.uri="mongodb://localhost:27017"

# Specific test class
./gradlew :driver-core:test --tests "com.mongodb.internal.operation.FindOperationTest"

# Alternative JDK
./gradlew test -PjavaVersion=11

# Scala tests (all versions)
./gradlew scalaCheck
```

## Module-Specific Notes

- **driver-core:** Largest test suite — JUnit 5 + Spock + Mockito
- **driver-sync:** JUnit 5 + Spock (heavy Spock usage, but don’t add new)
- **driver-reactive-streams:** JUnit 5 + Spock + Project Reactor
- **bson-scala / driver-scala:** ScalaTest, test per Scala version
- **Kotlin modules:** JUnit 5 + mockito-kotlin

## Integration Tests

- Require a running MongoDB instance
- Set connection URI: `-Dorg.mongodb.test.uri="mongodb://localhost:27017"`
- Integration test source set configured via `conventions/testing-integration.gradle.kts`

See [examples.md](examples.md) for Java, Scala, and Kotlin test skeletons.
