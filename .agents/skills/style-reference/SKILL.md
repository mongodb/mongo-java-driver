---
name: style-reference
description: Detailed code style rules for Java, Kotlin, Scala, and Groovy in the MongoDB Java Driver. Use when you need specific formatting rules beyond the basics in root AGENTS.md — e.g., line length, import ordering, brace style.
---
# Style Reference

## Java Style Rules

- **Max line length:** 140 characters
- **Indentation:** 4 spaces (no tabs)
- **Line endings:** LF (Unix)
- **Charset:** UTF-8
- **Star imports:** Prohibited (AvoidStarImport)
- **Final parameters:** Required (FinalParameters checkstyle rule)
- **Braces:** Required for all control structures (NeedBraces)
- **Else placement:** `} else {` on the same line (Palantir Java Format default)
- **Copyright header:** Every Java / Kotlin / Scala file must contain `Copyright 2008-present MongoDB, Inc.`
- **Formatter:** Palantir Java Format

## Kotlin Style Rules

- **Formatter:** ktfmt dropbox style, max width 120
- **Static analysis:** detekt

## Scala Style Rules

- **Formatter:** scalafmt
- **Supported versions:** 2.11, 2.12, 2.13, 3 (default: 2.13)

## Groovy Style Rules

- **Static analysis:** CodeNarc

## Javadoc / KDoc / Scaladoc

- All public classes and interfaces **must** have class-level Javadoc (enforced by checkstyle)
- Public methods should have Javadoc with `@param`, `@return`, and `@since` tags
- Use `@since X.Y` to indicate the version when the API was introduced
- Use `@mongodb.driver.manual <path> <label>` for MongoDB manual links
- Use `@mongodb.server.release <version>` to indicate the minimum server version required
- Scala modules use Scaladoc — follow Scaladoc conventions (`@param`, `@return`, `@since`, `@see`)
- Internal packages (`com.mongodb.internal.*`, `org.bson.internal.*`) are excluded from doc generation
- Run `./gradlew doc` to validate Javadoc/KDoc/Scaladoc builds cleanly

## Prohibited Patterns

- `System.out.println` / `System.err.println` — Use SLF4J logging
- `e.printStackTrace()` — Use proper logging/error handling

## Formatting Commands

```bash
./gradlew spotlessApply    # Auto-fix all formatting
./gradlew spotlessCheck    # Check without modifying
```
