# AGENTS.md - mongodb-crypt

Client-side field-level encryption (CSFLE) support via JNA bindings to libmongocrypt.

**Depends on:** `bson`

- Work here if: modifying CSFLE encryption/decryption logic or JNA bindings
- Do not: move, re-expose, or refactor JNA/libmongocrypt bindings without human approval — this is security-critical
  code
- Do not: modify the C API binding layer without understanding the libmongocrypt contract

## Key Packages

- `com.mongodb.crypt.capi` — mongocryptd C API bindings (JNA) — **security-critical, do not modify without human
  review**
- `com.mongodb.internal.crypt.capi` — Internal encryption state management

## Build & Test

```bash
./gradlew :mongodb-crypt:test
./gradlew :mongodb-crypt:check
```

Tests are primarily integration tests requiring libmongocrypt native libraries.
Build downloads JNA libs for multiple platforms, embedded in the JAR.
