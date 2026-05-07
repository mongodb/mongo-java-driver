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

## API Lifecycle

- **@Deprecated:** Deprecate in minor release, remove in next major. Always include `@deprecated Use {@link Replacement}` with a migration path.
- **@Alpha:** Early development, subject to incompatible changes or removal. Exempt from compatibility guarantees. Not for production use by applications; libraries must not depend on Alpha APIs.
- **@Beta:** Subject to incompatible changes or removal. Exempt from compatibility guarantees. Safe for applications (at the cost of extra upgrade work), but libraries should not depend on Beta APIs.
- **@Sealed:** Use when the driver provides internal implementations but consumers must not subclass (e.g., `ReadPreference`, `WriteConcern`).

## Module Ownership

- Public API lives in `driver-sync`, `driver-reactive-streams`, and language wrappers (`driver-kotlin-sync`, `driver-kotlin-coroutine`, `driver-scala`)
- `driver-core` owns shared internals, query builders (`Filters`, `Updates`, `Aggregates`), and the async execution layer
- Sync wrappers delegate to async core — never add sync-only logic that diverges from the async path

## Design Principles

These guide the driver's API surface:

- **Deep modules:** Prefer simple interfaces with powerful implementations over shallow wrappers.
- **Information hiding:** Bury complexity behind simple interfaces.
- **Pull complexity downward:** Make the implementer work harder so callers work less.
- **General-purpose over special-case:** Fewer flexible methods over many specialized ones.
- **Define errors out of existence:** Design APIs so errors cannot happen rather than detecting and handling them.

## Key Patterns

- **Static factory methods:** `Filters.eq()`, `Updates.set()`, `Aggregates.match()` — prefer these over constructors for public API
- **Fluent builders:** `MongoClientSettings.builder()` is the primary entry point — use for any class with more than 2–3 configuration options
- **Abstract core with pluggable transports:** `driver-core` defines operations; transport modules (`driver-sync`, `driver-reactive-streams`) execute them
- **Immutable value objects:** `MongoNamespace`, `WriteConcern`, `ReadPreference` are immutable — modifications return new instances
