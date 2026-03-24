# AGENTS.md - mongodb-crypt

Client-side field-level encryption (CSFLE) support via JNA bindings to libmongocrypt.

**Depends on:** `bson`

## Key Packages

- `com.mongodb.crypt.capi` — mongocryptd C API bindings (JNA)
- `com.mongodb.internal.crypt.capi` — Internal encryption state management

## Notes

- Every package must have a `package-info.java`
- Tests are primarily integration tests requiring libmongocrypt native libraries
- Build downloads JNA libs for multiple platforms, embedded in the JAR
- **Security-critical module — changes require careful review**
