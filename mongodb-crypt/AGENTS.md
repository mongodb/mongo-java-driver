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

## CAPI.java — JNA Binding Declarations

`CAPI.java` declares the JNA native method signatures for libmongocrypt. Javadoc on each method
must match the documentation in [mongocrypt.h](https://github.com/mongodb/libmongocrypt/blob/master/src/mongocrypt.h).

When adding or updating bindings:
- Copy the doc comment from `mongocrypt.h` verbatim
- Replace Doxygen `@ref type_name` with Javadoc `{@link type_name}` (for types) or
  `{@link #function_name}` (for functions in the same class)
- Replace `@p param_name` with `{@code param_name}`

## Build & Test

```bash
./gradlew :mongodb-crypt:test
./gradlew :mongodb-crypt:check
```

Tests are primarily integration tests requiring libmongocrypt native libraries.
Build downloads JNA libs for multiple platforms, embedded in the JAR.
