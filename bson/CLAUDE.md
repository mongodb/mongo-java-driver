# CLAUDE.md - bson

Core BSON (Binary JSON) library. This is the foundation module — all other modules depend on it.

## Key Packages

- `org.bson` — Core BSON value types (`BsonDocument`, `BsonValue`, `BsonReader`, `BsonWriter`)
- `org.bson.codecs` — Codec framework (`Encoder`, `Decoder`, `Codec`)
- `org.bson.codecs.configuration` — Codec registry and provider infrastructure (`CodecRegistry`, `CodecProvider`)
- `org.bson.codecs.pojo` — POJO codec support with conventions and property modeling
- `org.bson.codecs.jsr310` — Java 8+ date/time codec support
- `org.bson.json` — JSON conversion (`JsonReader`, `JsonWriter`, `JsonMode`)
- `org.bson.io` — Binary I/O (`ByteBuffer`, `OutputBuffer`)
- `org.bson.types` — Legacy types (`ObjectId`, `Decimal128`, etc.)
- `org.bson.internal` — Private API (vector support, encoding utilities)

## Notes

- Every package must have a `package-info.java`
- JUnit 5 + Spock (Groovy) tests in both unit and functional dirs

## Key Patterns

- Codec-based serialization with type-safe `BsonValue` hierarchy
- `BsonDocument` implements `Map<String, BsonValue>`
- All public types in `org.bson` — internal types in `org.bson.internal`
