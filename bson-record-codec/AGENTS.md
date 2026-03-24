# AGENTS.md - bson-record-codec

Java record codec support for BSON serialization.
**Requires Java 17+.**

**Depends on:** `bson`

- Work here if: modifying Java record serialization support

## Key Packages

- `org.bson.codecs.record` — `RecordCodecProvider` and record field accessors

## Build & Test

```bash
./gradlew :bson-record-codec:test
./gradlew :bson-record-codec:check
```

For global rules see [root AGENTS.md](../AGENTS.md).
