# AGENTS.md - driver-legacy

Legacy MongoDB Java driver API — backwards compatibility facade for the 3.x API.

**Depends on:** `bson`, `driver-core`, `driver-sync`

- Work here if: fixing bugs in the legacy 3.x API compatibility layer
- Do not: add new features (new functionality goes in `driver-sync` or `driver-core`)
- Do not: add new Spock tests

## Key Packages

- `com.mongodb` — Legacy API (`MongoClient`, `DB`, `DBCollection`, `DBCursor`) — distinct from `driver-core` classes in
  the same namespace
- `com.mongodb.gridfs` — Legacy GridFS

## Build & Test

```bash
./gradlew :driver-legacy:test
./gradlew :driver-legacy:check
```
