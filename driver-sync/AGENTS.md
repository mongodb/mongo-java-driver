# AGENTS.md - driver-sync

Synchronous (blocking) Java driver — the most commonly used entry point for Java applications.

**Depends on:** `bson`, `driver-core`

- Work here if: modifying the blocking Java API surface (`MongoClient`, `MongoDatabase`, `MongoCollection`), GridFS, or
  CSFLE vault client
- Do not: add new Spock tests (legacy only), add internal connection/protocol code (that belongs in `driver-core`)

## Key Packages

- `com.mongodb.client` — Public sync API
- `com.mongodb.client.gridfs` — GridFS sync implementation
- `com.mongodb.client.vault` — Client-side field-level encryption client

## Build & Test

```bash
./gradlew :driver-sync:test
./gradlew :driver-sync:check
```

Primary entry point: `MongoClients.create()` or `MongoClients.create(MongoClientSettings)`.
