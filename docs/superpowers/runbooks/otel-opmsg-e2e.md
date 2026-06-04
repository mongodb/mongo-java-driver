# Runbook: OTel OP_MSG propagation end-to-end (manual)

Validates DRIVERS-3454 end to end: a sync-driver client span linked to a server child span.

## Prerequisites
- A local build of the server POC branch (10gen/mongo PR #49930), which accepts OP_MSG
  section kind 3 and starts a server span from it. NOTE: the POC does NOT yet advertise
  `tracingSupport` in hello. Until SERVER-107128 adds it, temporarily force the driver
  capability on for this manual run (see step 3).
- An OpenTelemetry collector / exporter the server POC is configured to export traces to
  (e.g. Jaeger via OTLP), plus a Micrometer Tracing OTel bridge on the client.

## Steps
1. Build & run the server POC `mongod` with tracing enabled and sampling at 100%.
2. Start a Jaeger all-in-one (or OTLP collector) and point both server and client exporters at it.
3. In a scratch sync-driver program, configure a `MongoClient` with an `ObservationRegistry`
   that has an OTel-backed `DefaultTracingObservationHandler` (so `traceParent()` is non-null
   and sampled). Run a `find`.
   - Temporary capability override for the run (POC server lacks the hello flag): start an
     OTel root span yourself, then run the command. Because the server POC accepts kind 3
     unconditionally, set the driver `tracingSupported` to true by connecting to a server
     whose hello you patch to include `tracingSupport: true`, OR temporarily hardcode
     `isTracingSupport()`/`tracingSupported` to `true` on the local branch for the manual run
     only (revert before staging).
4. In Jaeger, confirm a single trace contains BOTH the client `find` span and a server span,
   with the server span's parent = the client span id sent in the traceparent.

## Pass criteria
- One trace, two+ spans, correct parent/child linkage across the client→server boundary.
- With the driver capability off (default), no server span is created (negative check).

## Notes
- This proves the wire format end to end; the automated Phase 1 test
  (`CommandMessageOtelTraceContextTest`) is the regression guard. In particular,
  `getCommandDocumentIgnoresOtelSection` guards against the trailing kind-3 section
  corrupting command-document reconstruction on the send path.
- Findings (any format mismatch, tracestate handling, flags) feed back into the spec.
