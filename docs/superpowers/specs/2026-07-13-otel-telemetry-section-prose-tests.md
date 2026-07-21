# Trace-Context Propagation Prose Tests (DRIVERS-3454)

These tests complement the OpenTelemetry spec prose tests. Tests 1–4 are unit-level and
MUST NOT require a server. Test 5 requires a MongoDB 9.0+ server (maxWireVersion >= 29)
started with OTel tracing enabled and the OTLP file exporter
(`--setParameter opentelemetryTraceDirectory=<dir>` plus the tracing feature flag and
sampling parameters), running on the same host as the test.

## 1. Telemetry section is attached when supported and traced

With tracing enabled and an active operation span, encode a command targeting a
connection whose `maxWireVersion` is >= 29. Assert the resulting OP_MSG contains exactly
one section of kind 3 whose payload is a BSON document of the form
`{otel: {traceparent: <string>}}`, where `traceparent` is 55 characters,
`00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`, and the trace-id/span-id match
the **command span**'s context.

## 2. Telemetry section is omitted for older servers

Same as (1) but with `maxWireVersion` < 29. Assert no section of kind 3 is present.

## 3. Telemetry section is omitted without an active span

With tracing disabled (or no active span, e.g. monitoring/auth commands), encode a
command at `maxWireVersion` >= 29. Assert no section of kind 3 is present.

## 4. Malformed trace context is never sent

With an active span whose context cannot produce a valid W3C traceparent (zero trace-id,
zero span-id, wrong-length or non-lowercase-hex ids), assert no section of kind 3 is
present (drivers MUST omit the section rather than send an invalid traceparent).

## 5. End-to-end server span linkage

With tracing enabled, run a CRUD operation (e.g. `find`) against the configured 9.0+
server. Capture the driver's finished spans (client side). Then read the server's
exported OTLP JSON from the trace directory and assert a server span exists whose
`traceId` equals the client trace-id and whose `parentSpanId` equals the client
**command** span's span-id (the traceparent is that of the command span, passed to
`CommandMessage.encode(...)` per the 2026-07-18 command-span propagation design;
`OperationContext.getTracingSpan()` still returns the operation span, used only for
parenting the command span — so the server span is a direct child of the client command
span). Allow for the server's batch export interval when polling.
