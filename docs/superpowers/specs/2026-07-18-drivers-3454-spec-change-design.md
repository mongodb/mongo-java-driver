# Design: DRIVERS-3454 Minimal Spec Change (mongodb/specifications)

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454)
- **Status:** Approved design
- **Author:** Nabil Hachicha
- **Date:** 2026-07-18
- **Based on:** the reference implementation on branch `nabil_otel_context` (see
  `2026-07-13-otel-telemetry-section-reference-impl-design.md`) and the shipped server contract
  ([10gen/mongo#56646](https://github.com/10gen/mongo/pull/56646)).

## Goal

Draft the minimal normative spec change reflecting DRIVERS-3454 (OTel trace-context propagation to the server),
split across the two specs that own each layer, in the `mongodb/specifications` repo (worked on locally in the
`testing/resources/specifications` submodule).

## Where the work happens

- Branch **`DRIVERS-3454`** in `testing/resources/specifications`, based on **`origin/master`** (the repo's default
  branch; there is no `main`).
- Local only; nothing pushed. The parent repo's submodule pointer is NOT updated (the parent repo treats the
  submodule as do-not-modify; this work is on an explicit user instruction and stays on a local branch).
- Conventions per the repo's `AGENTS.md`: GitHub Flavored Markdown, 120-character line width, MongoDB docs style,
  dated changelog entries prepended inside each spec's `## Changelog`.

## Change 1 — `source/message/OP_MSG.md` (wire layer)

1. Extend the `Section` struct's union:

   ```c
   document telemetry; // payloadType == 3
   ```

2. Add normative rules next to the existing payload-type rules:
   - An `OP_MSG` MAY contain **at most one** section with Payload Type 3.
   - Payload Type 3 is **request-only**: drivers MAY send it; it MUST NOT appear in server replies.
   - Its payload is a single BSON document whose contents are defined by the
     [OpenTelemetry specification](../open-telemetry/open-telemetry.md) (cross-link).
   - Receivers that do not recognize a section kind fail the message (existing behavior), therefore senders MUST
     gate emission on server support as defined by the OpenTelemetry specification.
3. Include a brief explanatory line on numbering: **Payload Type 2 is reserved for server-internal use** (a
   security-token section populated by the server/infrastructure components, never by drivers), so the next
   available payload type for driver-emitted content is 3. Kind 2 itself remains undocumented here (status quo,
   out of scope).
4. Changelog entry dated 2026-07-18.

## Change 2 — `source/open-telemetry/open-telemetry.md` (driver behavior)

1. New subsection under Implementation Requirements: **"Propagating Trace Context to the Server"**:
   - Payload schema: `{ otel: { traceparent: <string> } }`, where `traceparent` is the W3C traceparent value of the
     **operation span** (stated explicitly: drivers attach the context at message-encode time, before the command
     span exists; server spans therefore join the trace as children of the operation span).
   - Drivers attach the section **iff**: tracing is enabled, the connection's `maxWireVersion >= 29` (MongoDB 9.0),
     and a valid traceparent exists.
   - Validity mirrors server-side enforcement: exactly 55 characters,
     `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`, non-zero trace-id and parent-id. Drivers MUST omit
     the section rather than send an invalid value; MUST NOT append `tracestate`; unsampled contexts are propagated
     with trace-flags `00`.
   - At most one telemetry section per message. Connections without an active span (monitoring, authentication)
     naturally send no section (informative note).
2. **Design Rationale** additions (brief):
   - Wire-version gate instead of a `hello` capability flag: the wire protocol version acts as the schema contract
     for the payload; future fields require a wire-version bump (decision of 2026-06-16).
   - BSON document instead of a bare traceparent string: extensibility without redesigning the payload format.
3. Changelog entry dated 2026-07-18.
4. **No Future Work addition** (explicitly out of scope per review).

## Out of scope

- Test-plan / prose-test additions (`tests/README.md`) — normative text only; tests follow in a later PR.
- Unified test format schema changes; YAML/JSON test files (so no `make -C source` regeneration needed).
- Documenting Payload Type 2 beyond the one-line reservation note.
- Pushing the branch or updating the parent repo's submodule pointer.

## Validation

- Run available repo checks locally (`pre-commit run --files <changed>` and/or `mkdocs build --strict`) if the
  tooling is installed; otherwise verify 120-char width and markdown lint manually and note it.

---

## Decision record (2026-07-21): spec prose test — chosen candidate, deferred

**Decision:** the spec PR remains normative-text-only for now; no prose test was added to
`source/open-telemetry/tests/README.md`. When the DRIVERS review asks for a test plan (the PR template expects
one), the agreed candidate is a single appended prose test (*Test 3*, numbering is load-bearing — existing tests
1–2 must not be renumbered):

> **Test 3: Trace Context Propagation Does Not Affect Command Execution.** With tracing enabled, run CRUD
> operations across the standard server-version/topology matrix, on both sides of the wire-version gate
> (`maxWireVersion` < 29 and >= 29), and assert the operations succeed and spans are emitted.

**Why this candidate (and not the alternatives):** cross-driver prose tests can only rely on observables every
driver has. The telemetry section is invisible to command monitoring/APM (transport-layer), and server spans are
observable only with special mongod startup parameters (OTLP exporter + feature flag) that no driver CI matrix
provisions. That rules out prescribing (a) wire-level gating-matrix assertions (driver-internal byte inspection)
and (b) the full server-span-linkage e2e (infra burden other driver teams would reject). The chosen test instead
uses the **server's strict validation as the oracle**: a 9.0+ server `uasserts` on any malformed traceparent and
a pre-9.0 server rejects unknown OP_MSG section kinds, so plain command success across the matrix simultaneously
verifies the wire-version gate and the well-formedness of anything sent — and directly tests the spec's key
operational property that telemetry must never become an availability failure. It runs on existing CI with zero
new infrastructure.

**Known limitation (accepted):** the test cannot prove the section is ever *sent* — a driver that never attaches
the section passes trivially (false-negative tolerant). Positive proof of emission/omission stays in
driver-internal unit tests (in the Java reference implementation: the `CommandMessageOtelTraceContextTest`
OP_MSG round-trip matrix and the `ServerSpanLinkageProseTest` e2e, see
`docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md`); the spec test text should recommend —
not mandate — equivalent driver-level verification.
