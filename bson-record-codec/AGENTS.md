# AGENTS.md - bson-record-codec

Java record codec support for BSON serialization.
**Requires Java 17+.**

**Depends on:** `bson`

- Work here if: modifying Java record serialization support
- This module targets **Java 17** (`sourceCompatibility = 17`) — Java 17 features (records, sealed classes, text blocks,
  etc.) are permitted here, unlike most other modules which target Java 8

## Key Packages

- `org.bson.codecs.record` — `RecordCodecProvider` and record field accessors

## Build & Test

```bash
./gradlew :bson-record-codec:test
./gradlew :bson-record-codec:check
```
