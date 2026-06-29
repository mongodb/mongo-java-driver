# QE Prefix/Suffix GA + rename Text→String API

**Ticket:** JAVA-6168 (Java split of DRIVERS-3321, under epic DRIVERS-3408)
**Date:** 2026-06-29
**Status:** Approved design

## 1. Summary

Promote Queryable Encryption (QE) prefix/suffix text queries from preview to GA in the
Java driver, and rename the public explicit-encryption API from "Text" to "String", matching
the spec changes in DRIVERS-3321 and the Node analog PR
[#4972](https://github.com/mongodb/node-mongodb-native/pull/4972). Substring stays as an
experimental preview carry-over.

This rides on the libmongocrypt bump from 1.18.1 → **1.19.1** (already applied on the branch).

## 2. Ticket hierarchy and scope

- **DRIVERS-3408** is the epic ("QE Prefix/Suffix/Substring GA").
- **DRIVERS-3321** ("Add QE Prefix+Suffix GA and rename API to string") is the implementation
  child. Its Java split is **JAVA-6168** (this work). DRIVERS-3321 is **not** a parallel track —
  it *is* the work for the epic in Java. Node PR #4972 (NODE-7537) is the exact analog.

### In scope
- libmongocrypt 1.19.1 bump (done).
- Public API rename Text→String in `driver-core`, with `@Deprecated` aliases for the old names.
- Substring carried over as experimental preview (matches Node).
- Rewrite the QE text explicit-encryption prose test to the GA spec ("27. String Explicit
  Encryption"); rely on the auto-discovered `QE-Text-*` unified spec tests.

### Out of scope (separate, blocked, or future — not doable on 1.19.1)
- **DRIVERS-3540** — substring *GA* rename (`substringPreview` → `substring`). Blocked on a
  server/libmongocrypt rename; libmongocrypt 1.19.1 only ships `substringPreview`.
- **DRIVERS-3548** — drop the preview query types. Scheduled near server 9.0, future.
- **DRIVERS-3294** — update the `$lookup` prose test 25 for MONGOCRYPT-793 (Node split NODE-7221).
  Unrelated to text/string queries (it tests encrypted-collection joins). Its own JAVA ticket/PR.

## 3. libmongocrypt 1.19.1 behavior (source-verified)

Verified against `mongocrypt-ctx.c` at tag `1.19.1`.

| Identifier | Kind | 1.19.0 | 1.19.1 | Notes |
|---|---|---|---|---|
| `prefixPreview` / `suffixPreview` | query type | removed | **restored** | Work on pre-9.0 servers; persisted in `encryptedFields` |
| `prefix` / `suffix` | query type | added (GA) | GA | Server 9.0.0+ |
| `substringPreview` | query type | preview | preview | Still experimental; no GA yet |
| `textPreview` | algorithm | renamed → `string`, **not** restored | still gone | Client-only selector |
| `string` | algorithm | added (GA) | GA | `MONGOCRYPT_ALGORITHM_STRING_STR` |

`mongocrypt_ctx_setopt_algorithm()` matches (case-insensitive): Deterministic, Random, Indexed,
Unindexed, Range, RangePreview (→ explicit error), String. **There is no `textPreview` branch** —
passing it fails with `unsupported algorithm string "textPreview"`.

### Valid algorithm / queryType pairings (verified)
The init validation in `mongocrypt-ctx-encrypt.c` requires the `string` index type for all the
text query types (`prefix query type requires string index type`, etc.), and treats
`prefixPreview`/`suffixPreview` as deprecated aliases of `prefix`/`suffix`:

| algorithm | queryType | result |
|---|---|---|
| `"String"` | `"prefix"` (GA) | valid |
| `"String"` | `"prefixPreview"` (deprecated alias) | valid |
| `"String"` | `"substringPreview"` (experimental) | valid |
| `"TextPreview"` | *anything* | rejected — `unsupported algorithm string "textPreview"` |

So the only way to use `prefixPreview`/`suffixPreview` after this work is with the `"String"`
algorithm — the old `"TextPreview"` + `"prefixPreview"` combination no longer exists. For
**explicit** encryption, `string`+`prefix` and `string`+`prefixPreview` produce identical payloads;
the distinction matters only for **auto-encryption**, where the query-type name persisted in
`encryptedFields` must match the target server (pre-9.0 → `prefixPreview`, 9.0+ → `prefix`). The
`EncryptOptions.queryType` javadoc should state this pairing constraint.

### Why the asymmetry is intentional (not an oversight)
- **Query types** are persisted in a collection's `encryptedFields` schema and are
  server-version-dependent. Pre-9.0 servers (8.2/8.3) *only* support the preview names; 9.0
  rejects them. For **auto-encryption** against a pre-9.0 server, libmongocrypt must tolerate
  `prefixPreview`/`suffixPreview` in `encryptedFields` — hence the restore (MONGOCRYPT-937 #1191).
- **The algorithm name** is a client-only selector that picks the internal index type
  (`MONGOCRYPT_INDEX_TYPE_STRING`) for explicit encryption. It is never written to
  `encryptedFields` or sent to the server, so there is nothing deployed that references
  `textPreview`. A one-time rename costs nothing and needs no restore (MONGOCRYPT-870 #1184). The
  target `string` (over `text`) was chosen per DRIVERS-3531 to avoid confusion with MongoDB text
  search.

## 4. Public API changes — `driver-core` (`com.mongodb.client.model.vault`)

- **New `StringOptions` class** (`@Alpha(Reason.SERVER)`): same shape as today's `TextOptions`
  (`caseSensitive`, `diacriticSensitive`, `prefixOptions`, `suffixOptions`, `substringOptions`).
  `substringOptions` documented as experimental/preview.
- **`TextOptions` retained as `@Deprecated`**, as a deprecated alias of `StringOptions`. Preferred
  shape: `TextOptions extends StringOptions` with no new members, so existing code compiles and any
  `TextOptions` instance is usable where `StringOptions` is expected. The exact binary-safe form
  (subclass vs. standalone delegating class) is confirmed during planning.
- **`EncryptOptions`**: add `stringOptions(StringOptions)` + `getStringOptions()`; retain
  `textOptions(TextOptions)` + `getTextOptions()` as `@Deprecated`, backed by the same field.
  Resolution mirrors Node (`stringOptions` wins, falling back to `textOptions`).
- **Doc-only updates** to `EncryptOptions`: algorithm list `"TextPreview"` → `"String"`; queryType
  docs `prefixPreview`/`suffixPreview` → `prefix`/`suffix` (note the preview names remain supported
  for pre-9.0 servers; `substringPreview` experimental).

### Algorithm caveat
The `algorithm` value is a free-form `String` supplied by the user and passed straight to
libmongocrypt, which removed `"textPreview"` (only `"string"` works). We therefore **cannot**
provide a driver-side code alias for the algorithm string — we document `"String"` as the GA name.
This is why the existing prose test (`new EncryptOptions("TextPreview")`) was disabled and must be
rewritten to `"String"`.

### Binary compatibility
All changes are additive (new class, new methods) or retain deprecated members. **No breaking
change; no major-version bump required.**

## 5. Internal mapping & JNA — `mongodb-crypt` (minimal)

- `EncryptOptionsHelper`: read from `getStringOptions()` (falling back to deprecated
  `getTextOptions()`); BSON keys (`prefix`/`suffix`/`substring`, `caseSensitive`,
  `diacriticSensitive`) are **unchanged**.
- **No JNA binding change required.** The C function
  `mongocrypt_ctx_setopt_algorithm_text(ctx, opts)` and its BSON option schema are identical in
  1.19.1. Optional: refresh doc-comment wording ("TextPreview" → "String"). `CAPI.java`,
  `MongoCryptImpl`, `MongoExplicitEncryptOptions` need no functional change.

## 6. Test changes

- **Spec submodule:** already bumped; includes `QE-Text-prefix/suffix*` (GA) and `*Preview`
  files plus the `-ci-di` / `-preview` `encryptedFields` data.
- **Unified runner** (`ClientSideEncryptionTest`, sync + reactive): auto-discovers and runs the new
  `QE-Text-*.json` unified tests — largely free coverage.
- **Prose test rewrite** (`AbstractClientEncryptionTextExplicitEncryptionTest`): rewrite to spec
  test "27. String Explicit Encryption":
  - Switch to algorithm `"String"` + `stringOptions`/`StringOptions`.
  - Parameterize each case across `(prefix, prefix-suffix)` on server 9.0+/libmongocrypt 1.19.0+
    and `(prefixPreview, prefix-suffix-preview)` on server pre-9.0/libmongocrypt 1.19.1+.
  - Add the `-ci-di` (case-/diacritic-insensitive) and `-preview` collections; substring carried
    over as preview.
  - Restore proper `assumeTrue` gating (currently commented out).
  - Bump `REQUIRED_LIB_MONGOCRYPT_VERSION` to match the spec's per-case requirements.
  - Update the case-7 error substring (now the `"string"` algorithm).

### Auto-encryption pre-9.0 preview path — coverage notes
The auto-encryption path against a pre-9.0 server with `prefixPreview`/`suffixPreview`
`encryptedFields` **is** covered by the unified tests `QE-Text-prefixPreview` /
`QE-Text-suffixPreview` (`minServerVersion 8.2.0`, `maxServerVersion 8.99.99`, `csfle`
auto-encryption, no `bypassQueryAnalysis`, full insert+query round-trip). No new prose test needed.
Two execution conditions must hold, to verify during implementation:
1. These unified tests are **not skipped** by the Java runner / `UnifiedTestModifications`
   (they require libmongocrypt 1.19.1+, which we now have).
2. The **CI test matrix includes a pre-9.0 server (8.2/8.3)** — otherwise these `maxServerVersion
   8.99.99` tests silently no-op and the path is "covered" on paper but never executed.

## 7. Language wrappers

Kotlin and Scala reuse `driver-core`'s `EncryptOptions`/`TextOptions` directly (no wrapper copies
found), so they inherit `stringOptions`/`StringOptions` automatically. Verify during planning.

## 8. Risks

- Prose-test version gating (GA vs. preview keyed on the server-9.0 boundary and libmongocrypt
  1.19.0/1.19.1) is the trickiest part — must match the spec precisely.
- Users still passing `"TextPreview"` as the algorithm get a libmongocrypt error on 1.19.1+ —
  inherent to the bump, documented, not preventable in driver code.
- Pre-9.0 server absent from CI would hide the auto-encryption preview-path coverage (see §6).
