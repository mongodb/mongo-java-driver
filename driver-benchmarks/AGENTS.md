# AGENTS.md - driver-benchmarks

Performance benchmarks for the MongoDB Java Driver using JMH and a custom benchmark framework.

**Depends on:** `driver-sync`, `mongodb-crypt`

## Key Packages

- `com.mongodb.benchmark.benchmarks` — Benchmark suite and individual benchmarks (BSON, single/multi-doc, bulk
  operations)
- `com.mongodb.benchmark.benchmarks.bulk` — Bulk write benchmarks
- `com.mongodb.benchmark.benchmarks.netty` — Netty transport benchmarks
- `com.mongodb.benchmark.framework` — Custom benchmark harness
- `com.mongodb.benchmark.jmh` — JMH microbenchmarks

## Notes

- Not published — internal testing only
- Non-standard source layout: `src/main` (no `java` subdirectory)
- Run benchmarks: `./gradlew :driver-benchmarks:run` or `./gradlew :driver-benchmarks:jmh`
- Requires `-Dorg.mongodb.benchmarks.data` and `-Dorg.mongodb.benchmarks.output` system properties
