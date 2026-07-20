# Design: Propagate the Command Span's Trace Context (Option A)

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454)
- **Status:** Approved design (chosen in discussion 2026-07-18; "split creation from decoration" variant)
- **Author:** Nabil Hachicha
- **Date:** 2026-07-18
- **Amends:** `2026-07-13-otel-telemetry-section-reference-impl-design.md` (which recorded operation-span
  propagation as a limitation) and the spec branch `DRIVERS-3454` in `testing/resources/specifications`.

## Problem

The telemetry section currently propagates the **operation** span's traceparent because
`InternalStreamConnection.sendAndReceiveInternal` encodes the message (`:445`) before creating the command span
(`:448`), and `TracingManager.createTracingSpan` eagerly re-parses the **encoded** bytes
(`commandDocumentSupplier.get()` → `message.getCommandDocument(bsonOutput)`) just to obtain the command name,
sensitive-command verdict, and `getMore` cursor id. Server spans therefore parent to the operation span — siblings
of the command span instead of children — losing per-command / per-retry attribution.

## Key insight

All *eager* inputs of `createTracingSpan` are available from the **raw, un-encoded** command document that
`CommandMessage` already stores (`CommandMessage.java:96`): first key = command name; sensitive check needs only
the name; `getMore` cursor id is a field of the raw document. The only encode-dependent input, the folded command
document for `db.query.text`, is already consumed lazily *after* span creation (`setQueryText`). So span creation
can move before `encode()` with no loss.

In the Micrometer Observation model, span-id allocation and observation start are the same event, so
"pre-allocating ids" and "starting the span earlier" are the same change; decoration (attributes, scope, query
text) stays where it is today.

## Changes (driver-core, internal only)

1. **`TracingManager.createTracingSpan`** — replace the `Supplier<BsonDocument> commandDocumentSupplier`
   parameter with the raw command document (or have the method read it from `message`). Body otherwise unchanged
   (name, sensitive check, namespace resolution, observation-context population, `getMore` cursor id, session
   fields).
2. **`CommandMessage`** — expose the raw command document (package-private accessor) for (1); add an `encode`
   overload `encode(ByteBufferBsonOutput, OperationContext, @Nullable Span commandSpan)`. The base
   `RequestMessage.encode(bsonOutput, operationContext)` signature is untouched (`CompressedMessage` and other
   message types keep it).
3. **`writeTelemetryContextSection`** — read the traceparent from the passed `commandSpan` instead of
   `operationContext.getTracingSpan()`. Null command span (tracing disabled, sensitive command, monitoring/auth)
   ⇒ section omitted. The operation span is still used for *parenting* the command span in `TracingManager`.
4. **`InternalStreamConnection`** — in both the sync (`:445`) and async (`:620`) paths: create the command span
   *before* `encode()`, pass it to the new overload, and wrap `encode` so that an encode failure ends the span
   with error status (today encode failures happen span-less). The one-way `send` path (`:508`) has no command
   span today; it passes `null` (no section — matches current behavior since w:0 fire-and-forget commands carry
   no command span).

## Behavior changes (all intentional)

- Server spans become children of the **command** span. Each retry attempt / each `getMore` gets its own command
  span, so server spans attribute to the exact wire command that caused them.
- The command span's duration now includes BSON serialization (unavoidable: ids ⇒ started; and defensible —
  serialization is work done for that command). Microseconds against a network round trip.
- Security-sensitive commands can never carry a telemetry section (null command span), even where an operation
  span exists — strictly safer than today.

## Spec/doc updates (same effort)

- **Spec branch `DRIVERS-3454`** (`testing/resources/specifications`, `source/open-telemetry/open-telemetry.md`):
  flip the normative text from operation span to **command span** ("the propagated context MUST be that of the
  command span; server spans join the trace as children of the command span"), and drop the encode-ordering
  parenthetical. Changelog wording stays under the same 2026-07-18 entry (branch not yet merged).
- **Prose-test definitions** (`docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md`): test 1
  and test 5 assert the *command* span's ids.
- **E2E test** (`ServerSpanLinkageProseTest`): assert `parentSpanId == command span id`
  (`getFinishedSpans().get(0)`, per `AbstractMicrometerProseTest` ordering).
- **Unit tests**: `CommandMessageOtelTraceContextTest` passes the span via the new `encode` overload; add a case
  that a null command span omits the section even when an operation span exists.

## Testing

- Rework unit tests as above; run `:driver-core:test` affected suites.
- Micrometer prose tests (`AbstractMicrometerProseTest`) must stay green (span counts/names unchanged; only start
  timing moves).
- Re-run the e2e server-span linkage test against the local 9.0 Docker server (runbook) and confirm the server
  span's `parentSpanId` equals the **command** span id.

## Out of scope

- `Tracer.nextSpan()`-based true id pre-allocation (requires abandoning the Observation pipeline / new public
  API).
- Option B (append-section-after-encode with header backpatch).
- Any public API change.
