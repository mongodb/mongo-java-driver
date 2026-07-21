# Design: DRIVERS-3454 Reference Implementation — OTel Trace-Context Propagation (BSON telemetry section)

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454) — Support trace context propagation to the server
- **Status:** Approved design (reference implementation)
- **Author:** Nabil Hachicha
- **Date:** 2026-07-13
- **Supersedes:** `2026-06-01-otel-opmsg-propagation-design.md` (POC design; wire contract and negotiation have since changed)
- **Related:** [DRIVERS-719](https://jira.mongodb.org/browse/DRIVERS-719) (client-side OTel tracing),
  [SERVER-107128](https://jira.mongodb.org/browse/SERVER-107128),
  server implementation [10gen/mongo#56646](https://github.com/10gen/mongo/pull/56646) (merged 2026-07-06),
  [Technical Design: Drivers OTEL Trace Context Propagation to the Server](https://docs.google.com/document/d/1mLFw3yvLW3GPq8GLU82tNIHmsNiS5UvDmMJ4FjB80R4/edit)

---

## 1. Summary

Turn the existing POC on this branch into the production-quality **reference implementation**
for propagating the client's OTel trace context to the server, matching the final
driver/server contract that the server has now shipped (10gen/mongo#56646). The final
contract differs from the POC in three material ways:

1. **Payload is a BSON document**, not a bare traceparent C-string:
   `{ otel: { traceparent: "<55-char W3C traceparent>" } }`.
2. **Gating is by `maxWireVersion >= 29`** (server 9.0), not a `hello` capability flag.
   The hello-flag approach was explicitly rejected (meeting 2026-06-16, Nabil/Jeff/Didier).
3. The server **strictly validates** the traceparent and enforces a 4 KB section size cap.

Deliverables: production internal implementation in `driver-core` (shared by sync and
reactive), prose-test definitions upstreamable to the future DRIVERS spec PR, unit tests,
and an automated end-to-end test that asserts server-span linkage via the server's OTLP
file exporter.

## 2. Server contract (authoritative, from merged PR #56646)

- OP_MSG section kind **3** (`kTelemetry`), payload is a single BSON document.
- Schema (IDL `TelemetryContextSection`, `strict: false` at both levels — unknown fields
  ignored):

  ```
  { otel: { traceparent: <string> } }
  ```

- Max section size **4096 bytes** (`kMaxTelemetrySectionSize`); larger payloads are
  rejected with `BSONObjectTooLarge`.
- `traceparent` is validated by `otel::traces::validateW3CTraceparent`:
  - exactly **55 characters**: `"<version(2)>-<trace-id(32)>-<parent-id(16)>-<flags(2)>"`;
  - lowercase hex only; `-` delimiters at fixed positions;
  - trace-id and parent-id must **not** be all zeroes;
  - version `ff` is forbidden;
  - **no tracestate suffix is accepted** (a >55-char value is rejected — this differs from
    the earlier server POC #49930, which appended tracestate).
- Multiple telemetry sections in one message are rejected.
- The server writes the section before the body when serializing, but the **parser is
  order-agnostic**; the driver may write it after the body.
- Consumption: `service_entry_point_shard_role.cpp` starts server spans from the received
  context; mongos forwards it on downstream remote calls.
- Server spans are exported via startup parameters
  `opentelemetryTraceDirectory` (OTLP JSON files) or `opentelemetryHttpEndpoint`
  (OTLP/HTTP), subject to the server tracing feature flag and sampling parameters.

## 3. Driver-side design

### 3.1 Gating rule

Attach the telemetry section to an outgoing OP_MSG **iff**:

1. `MessageSettings.getMaxWireVersion() >= ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION`
   (new constant, value **29**, matching `WIRE_VERSION_90 = 29` on the server); and
2. the `OperationContext` carries an active tracing `Span` whose context yields a
   **valid** W3C traceparent (non-null).

Tracing-disabled clients, monitoring connections, and auth commands never have an active
span (per the DRIVERS-719 implementation), so those exclusions fall out of condition 2
without extra plumbing. `MessageSettings` already carries `maxWireVersion`, so no new
capability plumbing is needed.

> **2026-07-18 amendment:** condition 2 now requires the **command span** rather than the
> operation span — see `2026-07-18-command-span-propagation-design.md` for the rationale
> and the updated spec text.

### 3.2 Wire encoding (`CommandMessage`)

In `writeOpMsg()`, after the body and any document-sequence sections, when the gating rule
passes, write:

- one byte `PAYLOAD_TYPE_3_TELEMETRY = 3`;
- the BSON document `{ otel: { traceparent: "<value>" } }` (raw BSON, no length prefix
  beyond the document's own).

Placement after the body is kept (simpler for our encoder; server parser is
order-agnostic). The existing POC change to `getCommandDocument()` (tolerating a trailing
non-sequence section when re-parsing our own message) is retained and adapted.

### 3.3 Traceparent production (`MicrometerTracer` / `TraceContext`)

`TraceContext.traceParent()` remains the internal accessor, tightened to guarantee it
never returns a value the server would reject:

- format `00-<32 lowercase hex>-<16 lowercase hex>-<02 flags>`, exactly 55 chars;
- return `null` for no-op spans, zero trace-id or span-id, or any malformed component
  (section is then omitted entirely — the driver never sends a malformed traceparent);
- unsampled contexts are propagated with flags `00` (the server applies its own
  sampling); no tracestate is ever appended.

### 3.4 Removals (POC artifacts)

- `OtelTracePropagationTestToggle` (force-send toggle) — deleted.
- `MessageSettings.tracingSupported` and its builder — deleted.
- `ConnectionDescription` / `DescriptionHelper` `tracingSupport` hello parsing — reverted.
- POC-era tests reworked to the new contract.

No public API changes; everything stays under `com.mongodb.internal.*`. Sync and reactive
drivers both inherit the behavior through the shared `driver-core` send path.

## 4. Testing

### 4.1 Unit tests (run everywhere)

- **Traceparent formatting matrix** (`MicrometerTracer`): valid sampled/unsampled spans,
  no-op span, zero ids, length exactly 55, lowercase hex.
- **`CommandMessage` OP_MSG round-trip** (rework of `CommandMessageOtelTraceContextTest`):
  - positive: wire version ≥ 29 + active span ⇒ section kind 3 present; payload BSON
    parses to `{otel: {traceparent: ...}}` matching the span's trace-id/span-id;
  - negative: wire version < 29 ⇒ no section; no span ⇒ no section; invalid traceparent ⇒
    no section; interaction with document-sequence sections preserved.

### 4.2 Prose tests (upstreamable)

A markdown prose-test definition (structured for the future `mongodb/specifications`
tracing spec PR) covering the gating matrix and the e2e linkage test, plus Java
implementations following the `AbstractMicrometerProseTest` pattern.

### 4.3 End-to-end server-span linkage test

- New integration test (sync, `AbstractMicrometerProseTest`-style) using
  `InMemoryOtelSetup` to capture the client command span (expected trace-id/span-id).
- Enabled only when `-Dorg.mongodb.test.otel.trace.dir=<path>` is set; skipped otherwise.
- Requires the test's MongoDB deployment (same host, as with mongo-orchestration in CI) to
  be a **server 9.0 build started with**
  `--setParameter opentelemetryTraceDirectory=<path>` plus the tracing feature flag and
  sampling parameters.
- The test runs a traced CRUD operation, polls the directory past the export interval
  (`openTelemetryTracingBatchExportIntervalMillis`, default 1 s), parses the OTLP JSON
  span files, and asserts a server span exists with `traceId` equal to the client span's
  trace-id and `parentSpanId` equal to the client command span's span-id.
- The local runbook (`docs/superpowers/runbooks/otel-opmsg-e2e.md`) is updated for the
  Evergreen 9.0 artifact and file-exporter workflow (Jaeger flow kept as an optional
  visual check).

## 5. Branch strategy

Merge `origin/main` into `nabil_otel_context` (currently 11 commits behind), keeping POC
history; implement the production version as new, logically separated commits. Remove
unrelated local noise (e.g. `prompt.txt`, stray modified test files) before starting.

## 6. Out of scope

- `baggage` / `tracestate` propagation (future payload evolution; requires wire bump).
- Unified test format runner extensions.
- Evergreen YAML task wiring for the e2e test (follow-up).
- Public API surface changes and mongos/load-balancer forwarding (server-side concern).

## 7. Risks / open items

- **Wire version N = 29** matches server master today; if the final DRIVERS spec picks a
  different gate before 9.0 GA, only the `ServerVersionHelper` constant changes.
- The server deploys parsing behind a feature flag (design Decision 5); a 9.0 server with
  the flag off still parses the section (parse path is unconditional in #56646), so the
  driver needs no flag awareness.
- Intermediaries (Atlas Proxy, Envoy, mongobetween) compatibility is tracked server-side
  under SERVER-128017; not a driver concern for this implementation.
