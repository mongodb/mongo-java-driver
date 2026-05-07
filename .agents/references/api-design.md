---
name: api-design
description: API stability annotations, design principles, and patterns for the MongoDB Java Driver. Use when adding or modifying public API surface — new classes, methods, interfaces, or changing method signatures.
---
# API Design

## API Stability Annotations

- `@Alpha` — Early development, may be removed.
  Not recommended for production use.
- `@Beta` — Subject to change or removal.
  It's not recommended for Libraries to depend on these APIs.
- `@Evolving` — May add abstract methods in future releases.
  Safe to use, but implementing/extending bears upgrade risk.
- `@Sealed` — Must not be extended or implemented by consumers.
  Safe to use, but not to subclass.
- `@Deprecated` — Supported until next major release but should be migrated away from.

## Design Principles

- **Deep modules:** Prefer simple interfaces with powerful implementations over shallow wrappers.
- **Information hiding:** Bury complexity behind simple interfaces.
- **Pull complexity downward:** Make the implementer work harder so callers work less.
- **General-purpose over special-case:** Fewer flexible methods over many specialized ones.
- **Define errors out of existence:** Design APIs so errors cannot happen rather than detecting and handling them.

## Key Patterns

- Static factory methods: `Filters.eq()`, `Updates.set()`, `Aggregates.match()`
- Fluent builders: `MongoClientSettings.builder()` is the primary entry point
- Abstract core with pluggable transports
