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

`CAPI.java` declares the JNA native method signatures for libmongocrypt. Javadoc on each
declaration that has a counterpart in `mongocrypt.h` must match the documentation in the header
of the libmongocrypt version the driver binds to (`downloadRevision` in
`mongodb-crypt/build.gradle.kts`), e.g.
[mongocrypt.h at 1.18.1](https://github.com/mongodb/libmongocrypt/blob/1.18.1/src/mongocrypt.h).

For declarations with a `mongocrypt.h` counterpart (native methods, typedef wrapper classes):
- Copy the doc comment from the header verbatim — do not add, remove, or reword sentences,
  even when extra context (e.g. cross-references to related setopt functions) seems helpful
- Convert Doxygen markup to Javadoc, keeping the result valid for the `javadoc` tool
  (escape raw `<`/`>` as `{@code ...}` or HTML entities; paragraphs use `<p>`):
  - `@ref type_name` → `{@link type_name}` (for types) or `{@link #function_name}`
    (for functions in the same class)
  - `@p param_name` → `{@code param_name}`
  - `@param[in] name` / `@param[out] name` → `@param name`
  - `@returns` → `@return`
  - `@pre condition` → a `<p>Requires that condition.` paragraph (keeping the condition text verbatim)
  - `@code{.c}` example blocks may be omitted
- Preserve the header's own quirks (e.g. a `@ref` pointing at an unexpected status function) —
  fidelity to the header beats local consistency

Declarations with no `mongocrypt.h` counterpart (e.g. the `cstring` helper, the legacy
`mongocrypt_opts_t` class) get Java-authored Javadoc; keep it accurate and note when a type is
retained only for backwards compatibility.

Verify the result still generates cleanly with `./gradlew :mongodb-crypt:javadoc`.

## Build & Test

```bash
./gradlew :mongodb-crypt:test
./gradlew :mongodb-crypt:check
```

Tests are primarily integration tests requiring libmongocrypt native libraries.
Build downloads JNA libs for multiple platforms, embedded in the JAR.
