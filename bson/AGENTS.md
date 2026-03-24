# AGENTS.md - bson

Core BSON (Binary JSON) library — the foundation module.
All other modules depend on it.

**Depends on:** None (foundation module)

- Work here if: adding/modifying BSON types, codecs, JSON conversion, or binary I/O
- Do not: expose `org.bson.internal` types in public API

## Key Packages

- `org.bson` — Core types (`BsonDocument`, `BsonValue`, `BsonReader`, `BsonWriter`)
- `org.bson.codecs` — Codec framework (`Encoder`, `Decoder`, `Codec`, `CodecRegistry`)
- `org.bson.codecs.pojo` — POJO codec support
- `org.bson.json` — JSON conversion (`JsonReader`, `JsonWriter`)
- `org.bson.internal` — Private API — do not expose

## Build & Test

```bash
./gradlew :bson:test
./gradlew :bson:check
```

For global rules see [root AGENTS.md](../AGENTS.md).
For API design see [api-design skill](../.agents/skills/api-design/SKILL.md).
