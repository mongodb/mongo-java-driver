# Design: OpenTelemetry Trace-Context Propagation over OP_MSG

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454) — Support trace context propagation to the server
- **Status:** Design (Investigating phase)
- **Author:** Nabil Hachicha
- **Date:** 2026-06-01
- **Related:** [DRIVERS-719](https://jira.mongodb.org/browse/DRIVERS-719) (client-side OTel tracing), [SERVER-107128](https://jira.mongodb.org/browse/SERVER-107128) (define trace context in OP_MSG), server POC [10gen/mongo#49930](https://github.com/10gen/mongo/pull/49930)

---

## 1. Summary

DRIVERS-719 added client-side OpenTelemetry spans to the MongoDB drivers, but those
spans stop at the client boundary: the server starts a fresh, disconnected trace. This
work closes the gap by **propagating the client's active trace context to the server on
each wire message**, so the server can start a *child* span and produce one continuous
client → server timeline.

Two deliverables, sequenced so the POC validates the spec:

1. **Spec proposal** — defines the wire contract and negotiation mechanism, written so it
   can become a `mongodb/specifications` PR shepherded through the DRIVERS process.
2. **Java driver POC** — a minimal, `driver-sync`-only implementation that exercises every
   claim in the spec. Ambiguities in the spec should surface as failing tests or awkward
   code; findings feed back into the spec. The POC is internal/throwaway quality and
   touches no public API.

---

## 2. Background & constraints

- **Why transport layer, not command BSON.** Per SERVER-107128, the trace context is
  carried in an OP_MSG *section* rather than inside the command document, so the server
  can read it before parsing the command — enabling tracing of command-parse failures and
  early network handling.
- **The server POC is a prototype.** PR #49930 is explicitly throwaway ("USING AI DO NOT
  COPY"), intra-server focused, and unconditionally accepts the new section. It is the
  authoritative reference for the **wire format only**, not for negotiation or production
  behavior.
- **Process.** Spec changes must flow through DRIVERS tickets; the drivers team shepherds
  them. This document is the basis for that proposal.
- **Status of dependencies (June 2026).** No OP_MSG-propagation spec exists yet.
  SERVER-107128 is Open/Unassigned. The server `hello` capability flag described below is
  **not** in the POC and must be added server-side for the negotiation to work end to end.

---

## 3. Wire contract

### 3.1 OP_MSG section

A new OP_MSG section kind is defined:

| Kind | Name                  | Status     |
|------|-----------------------|------------|
| 0    | Body                  | existing   |
| 1    | Document Sequence     | existing   |
| 2    | Security Token        | existing   |
| **3**| **OTel Trace Context**| **new**    |

This matches `kOtelTelemetryContext = 3` in the server POC
(`src/mongo/rpc/op_msg.cpp`).

### 3.2 Payload format

The section payload is a single **null-terminated C-string** containing a **W3C
`traceparent`** value:

```
00-<32 hex trace-id>-<16 hex span-id>-<2 hex trace-flags>[-<tracestate>]
```

- `00` — W3C version.
- trace-id — 16 bytes, 32 lowercase hex chars.
- span-id — 8 bytes, 16 lowercase hex chars (the client span that created the RPC).
- trace-flags — 1 byte, 2 hex chars (e.g. `01` = sampled).
- Base length is **55 chars**; an optional `-<tracestate header>` suffix may follow
  (server POC appends `span_context.trace_state()->ToHeader()`).

The string is encoded with the BSON CString convention (UTF-8 bytes + trailing `\0`),
consistent with how the server reads it via `readCStr()`.

### 3.3 Direction & optionality

- **Request-only.** No trace data is added to responses; server spans are exported
  independently via the OTel pipeline and correlated by trace-id.
- **Sparse.** The section is present only when the client has an **active, sampled** span.
  When there is no span, or the span is not sampled, the section is omitted entirely.
- Server behavior (informational): if a trace context is present, the server starts a span
  subject to its own external tracing rate limiter.

---

## 4. Negotiation (the key addition over the POC)

Older or mixed-version servers `uassert(40432)` ("Unknown section kind") on an
unrecognized OP_MSG section. The driver therefore must **not** send section kind 3 to a
server that does not understand it.

**Mechanism:** the server advertises support in its `hello` response:

```
{ ..., "tracingSupport": true }
```

Rules:

- The driver reads `tracingSupport` from each `hello`/handshake response and stores it as a
  per-connection / per-server capability (default **false** when absent).
- The driver sends the section **only** on connections whose server advertised
  `tracingSupport: true`.
- The capability is re-evaluated on reconnect/failover; a server that stops advertising it
  (downgrade) stops receiving the section.

> **Open question / dependency.** This flag is not in server POC #49930. SERVER-107128 must
> add it. Until then, the POC validates the section via a controlled negotiation
> (see §6). An alternative considered was gating on a `maxWireVersion` bump; rejected for
> the spec because it couples the feature to a wire-version release and is coarser than an
> explicit capability. To be confirmed with the server team.

---

## 5. Edge cases (spec-level, noted; not all in POC scope)

- **Invalid/empty traceparent.** If the driver cannot produce a valid 55-char traceparent,
  it omits the section (never sends a malformed one). The server treats a sub-55-char or
  malformed value as absent.
- **tracestate size.** Pass-through only in the POC; the spec should reference W3C limits
  (512 chars) and define truncation/drop behavior — to be finalized with the server team.
- **Compression / OP_COMPRESSED.** The section is part of the OP_MSG body that gets
  compressed; no special handling expected, but the spec calls this out for confirmation.
- **mongos / load-balanced passthrough.** Out of scope for this driver work; the server is
  responsible for forwarding context to downstream services (noted only).

---

## 6. Java POC (driver-sync only)

Four focused changes in `driver-core`. All internal; no public API change.

### 6.1 Expose the trace context

`com.mongodb.internal.observability.micrometer.TraceContext` is currently an empty marker
interface, and `Span.context()` returns it. Extend it minimally:

```java
public interface TraceContext {
    TraceContext EMPTY = new TraceContext() { ... };
    @Nullable String traceParent();   // W3C traceparent, or null if unavailable/unsampled
}
```

- Implement in `MicrometerTraceContext`/`MicrometerSpan` by reading `traceId()`,
  `spanId()`, and `sampled()` from the underlying Micrometer `io.micrometer.tracing.TraceContext`
  and formatting the `00-…` string. Return `null` when the span is no-op or not sampled.
- `TraceContext.EMPTY` and `Span.EMPTY` return `null`.

### 6.2 Read the capability

- In `DescriptionHelper`, parse `tracingSupport` from the `hello` result.
- Store on `ServerDescription` (and `ConnectionDescription` as needed) with an
  `isTracingSupport()` accessor, following the existing `helloOk` / `maxWireVersion`
  patterns.

### 6.3 Inject the section

- In `CommandMessage`, add `PAYLOAD_TYPE_3_OTEL_TRACE_CONTEXT = 3`.
- In `writeOpMsg()`, after the body/sequence sections, write the new section **iff**:
  1. the connection's server advertised `tracingSupport`, **and**
  2. the active operation `Span` yields a non-null (sampled) `traceParent()`.
- The send path (`InternalStreamConnection.sendAndReceiveInternal`) already has both the
  `Span` and the connection description; thread the capability + traceparent into
  `CommandMessage` encoding via the existing plumbing.

### 6.4 Validation — phased

- **Phase 1 (in-repo, automated — the real proof):**
  - Positive: build the OP_MSG bytes for a command with an active sampled span on a
    tracing-capable connection; re-parse the sections and assert section kind 3 is present
    and its traceparent round-trips and matches the span's trace-id/span-id.
  - Negative: capability absent ⇒ no section; no/unsampled span ⇒ no section.
- **Phase 2 (optional, manual runbook — not CI):** build the server POC branch (#49930)
  locally with an OTel collector, point the sync driver at it, and confirm the server
  emits a child span linked to the client trace.

### 6.5 Scope guards (YAGNI)

Explicitly **out of scope** for the POC: reactive/async send path, any public API,
tracestate handling beyond pass-through, sampling rework, and `mongos`/load-balanced
propagation.

---

## 7. Testing summary

- Unit tests for traceparent formatting (`MicrometerTraceContext.traceParent()`), including
  unsampled and no-op cases.
- Unit test for `tracingSupport` parsing in `DescriptionHelper`.
- OP_MSG encode/parse round-trip tests in `CommandMessage` (positive + negative), per §6.4.
- All new code follows `driver-core` conventions (Java 8 baseline, SLF4J, copyright header,
  `@Nullable`/`@NonNull`). No reduction in coverage.

---

## 8. Deliverable order

1. Write & socialize this spec proposal (basis for the DRIVERS spec PR + SERVER-107128
   coordination on the `hello` flag).
2. Build the Java `driver-sync` POC (§6) and run Phase 1 validation.
3. Feed POC findings back into the spec; optionally run Phase 2 end-to-end.

---

## 9. Open questions

- Server `hello` `tracingSupport` flag ownership/timing (SERVER-107128 is unassigned).
- Final `tracestate` size/truncation policy.
- Whether this section should be designed as a special case or as the first user of the
  broader "client-side telemetry section" effort (retry metadata + OTel + client config).
- Confirmation that the W3C `traceparent` (vs. a BSON-structured payload) is the final
  on-wire encoding the server team will commit to.
