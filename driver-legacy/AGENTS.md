# AGENTS.md - driver-legacy

Legacy MongoDB Java driver API — backwards compatibility facade for the 3.x API.

**Depends on:** `bson`, `driver-core`, `driver-sync`

## Key Packages

- `com.mongodb` — Legacy API (`MongoClient`, `DB`, `DBCollection`, `DBCursor`) —
  distinct from `driver-core` classes in the same namespace
- `com.mongodb.gridfs` — Legacy GridFS

## Notes

- Do not add new features here — new functionality goes in `driver-sync` or
  `driver-core`
- Do not add new Spock tests
