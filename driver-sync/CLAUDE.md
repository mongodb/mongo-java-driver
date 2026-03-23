# CLAUDE.md - driver-sync

Synchronous (blocking) Java driver — the most commonly used entry point for Java applications.

**Depends on:** `bson`, `driver-core`

## Key Packages

- `com.mongodb.client` — Public sync API (`MongoClient`, `MongoDatabase`, `MongoCollection`)
- `com.mongodb.client.gridfs` — GridFS sync implementation
- `com.mongodb.client.vault` — Client-side field-level encryption client
- `com.mongodb.client.internal` — Implementation helpers (private API)

## Notes

- Every package must have a `package-info.java`
- JUnit 5 + Spock (Groovy). Spock is heavily used but do not add new Spock tests.
- Blocking wrapper over `driver-core` async infrastructure
- Primary entry point: `MongoClients.create()` or `MongoClients.create(MongoClientSettings)`
- Optional Micrometer integration for observability
