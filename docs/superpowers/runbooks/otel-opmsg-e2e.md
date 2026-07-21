# Runbook: OTel OP_MSG propagation end-to-end (manual)

Validates DRIVERS-3454 end to end against a real MongoDB 9.0/master server: a
sync-driver client trace linked to a server-side span created from the OP_MSG
telemetry section (section kind 3, `{otel: {traceparent: <string>}}`), via the
server's OTLP file exporter.

This supersedes the old POC-branch flow (10gen/mongo PR #49930). The server
feature landed as [10gen/mongo#56646](https://github.com/10gen/mongo/pull/56646)
(SERVER-129959, merged 2026-07-06) — no more `OtelTracePropagationTestToggle` /
`tracingSupport` hello-capability hacks are needed; the driver sends the
telemetry section whenever `maxWireVersion >= 29` (9.0+) and an active tracing
span yields a valid traceparent.

Last executed successfully: 2026-07-14, against
`gitVersion ee3a67f8f8735b5e4aedd2b66ccb700e92d55205`
(`9.0.0-alpha0-ee3a67f8`, mongodb-mongo-master mainline of 2026-07-14 —
after the #56646 merge), linux arm64 dist-test binary run in Docker on a macOS
arm64 host. `ServerSpanLinkageProseTest` PASSED.

Re-verified 2026-07-21 against a `9.0.0-alpha0` server in Docker container
`otel-opta` after the driver was changed to inject the traceparent from the
command span (rather than the operation span): `ServerSpanLinkageProseTest`
PASSED, with the server's `find` span `parentSpanId` equal to the client
command span's own span id.

## Prerequisites

- A `mongod` built from `10gen/mongo` `master` at or after the 2026-07-06 merge
  of #56646. Verify with `mongod --version` → `gitVersion` must be a master
  commit at/after that date; pre-merge binaries silently lack the telemetry
  section support.
- The automated test this runbook exercises:
  `driver-sync/src/test/functional/com/mongodb/client/observability/ServerSpanLinkageProseTest.java`
  (prose test 5 in
  `docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md`).
  It is gated by `@EnabledIfSystemProperty(named = "org.mongodb.test.otel.trace.dir", ...)`
  and skipped unless that property is set.

## Step 1: Get a server binary (what actually worked)

Do NOT submit new Evergreen patch builds for this — a no-op
`enterprise-macos-arm64` patch (and the same task on mainline) failed in
`bazel compile`, i.e. macOS `archive_dist_test` was broken on master at the
time of writing, and patch builds burn CI resources. Use an already-built
mainline artifact instead:

1. Authenticate REST calls with the CLI's OAuth token against the **corp**
   host (the public host rejects both static API keys and this token):

   ```bash
   TOKEN=$(evergreen client get-oauth-token)
   API=https://evergreen.corp.mongodb.com/api/rest/v2
   ```

   (If the CLI complains it is too old, `evergreen get-update` and use the
   downloaded binary.)

2. List recent mainline versions and pick a revision after 2026-07-06:

   ```bash
   curl -s -H "Authorization: Bearer $TOKEN" \
     "$API/projects/mongodb-mongo-master/versions?limit=40"
   ```

3. macOS arm64 variants are rarely activated on mainline; the reliably-green
   arm64 compile variant is `amazon-linux2023-arm64-static-compile`. Fetch its
   `archive_dist_test` task (task id pattern
   `mongodb_mongo_master_amazon_linux2023_arm64_static_compile_archive_dist_test_<rev>_<yy_mm_dd_hh_mm_ss>`,
   timestamp = the version's `create_time`) and take the presigned URL of the
   "Binaries" artifact (`mongo-<order>.tgz`, ~1 GB, unpacks to `dist-test/`):

   ```bash
   curl -s -H "Authorization: Bearer $TOKEN" "$API/tasks/<task_id>"   # → artifacts[].url
   curl -L -o mongodb-binaries.tgz "<Binaries url>"
   tar -xzf mongodb-binaries.tgz    # → dist-test/bin/mongod (aarch64 linux ELF)
   ```

   The binary used for the recorded run is kept at
   `~/MongoDB/otel-e2e-server/dist-test/bin/mongod`
   (from `mongodb-binaries-ee3a67f8.tgz` in the same directory).

4. Verify:

   ```bash
   docker run --rm -v ~/MongoDB/otel-e2e-server/dist-test:/opt/dist-test \
     otel-poc-mongod:latest /opt/dist-test/bin/mongod --version
   # db version v9.0.0-alpha0-ee3a67f8, gitVersion ee3a67f8...
   ```

   `otel-poc-mongod:latest` is a local `amazonlinux:2023` image with the
   runtime shared libs the dynamically-linked dist-test mongod needs
   (`openldap-compat` for `libldap_r`, cyrus-sasl, krb5, net-snmp, …); its
   Dockerfile is at `~/MongoDB/otel-poc-server/Dockerfile`.

## Step 2: Start mongod with tracing + the OTLP file exporter

Parameter names confirmed both from source
(`src/mongo/otel/traces/tracing_feature_flags.idl`, `trace_settings.idl`,
`trace_sampling_parameters.idl`) and by a live run — the plan's original
guesses (`featureFlagOtelTracing`, `opentelemetrySamplingRates`) were wrong.
Span creation requires **two** feature flags
(`src/mongo/otel/traces/tracing_enablement.cpp`): `featureFlagTracing`
(default true) AND `featureFlagOtelTraceSampling` (default false — set it).

Working invocation (Docker; note the host port remap — anything already
listening on host 27017, e.g. a local dev mongod, will otherwise shadow the
container because it binds 127.0.0.1 while Docker binds `*`):

```bash
mkdir -p /tmp/otel-e2e/db /tmp/otel-e2e/traces

docker run -d --name otel-e2e-mongod \
  -p 27227:27017 \
  -v ~/MongoDB/otel-e2e-server/dist-test:/opt/dist-test \
  -v /tmp/otel-e2e/db:/data/db \
  -v /tmp/otel-e2e/traces:/data/traces \
  otel-poc-mongod:latest \
  /opt/dist-test/bin/mongod --dbpath /data/db --port 27017 --bind_ip_all \
    --setParameter opentelemetryTraceDirectory=/data/traces \
    --setParameter featureFlagOtelTraceSampling=true \
    --setParameter 'openTelemetryTracingSampling={defaultSampling: {samplingFactor: 1.0}}'
```

(For a native binary, drop the Docker wrapper and point
`opentelemetryTraceDirectory` straight at `/tmp/otel-e2e/traces`.)

Sanity checks:

```bash
mongosh --quiet --port 27227 --eval '
  print(db.version());                                    // 9.0.0-alpha0-...
  print(db.adminCommand({hello:1}).maxWireVersion);       // 29
  printjson(db.adminCommand({getParameter:1, featureFlagOtelTraceSampling:1}));
  // -> currentlyEnabled: true
'
```

Notes:
- `openTelemetryTracingSampling` takes an `OpenTelemetryTracingSamplingConfig`
  BSON document, not a raw double; `samplingFactor: 1.0` samples everything
  (the default is 0.000045).
- The client's inbound traceparent is treated as an *externally sampled*
  trace, governed by `openTelemetryExternalTracing` (token bucket, defaults
  refillRate 1/s, maxTokens 10 — ample for a test run).
- Do NOT delete files under the trace directory while mongod runs: the file
  exporter keeps its `.jsonl` open and further exports go to the deleted
  inode. Restart the container (or mongod) after cleaning the directory.

## Step 3: Run the e2e test

```bash
./gradlew :driver-sync:test --tests 'com.mongodb.client.observability.ServerSpanLinkageProseTest' \
  -Dorg.mongodb.test.uri="mongodb://localhost:27227" \
  -Dorg.mongodb.test.otel.trace.dir=/tmp/otel-e2e/traces
```

Expected: PASSED. The test runs an instrumented `find`, then polls the trace
directory for an exported server span whose `traceId` equals the client trace
id and whose `parentSpanId` equals the driver's **command** span id.

Important semantics: the driver injects the traceparent from the
`OperationContext`'s active tracing span — the *command* span, per the
reference-impl design section 3.1 — because command-span creation was hoisted
above `CommandMessage.encode()` in `InternalStreamConnection.sendAndReceiveInternal()`.
The server `find` span is therefore a *direct child* of the client command
span. (An earlier iteration of the driver created the command span after
encoding, so the traceparent carried the operation span instead and the
server span was a sibling of the command span; the prose test was adjusted
accordingly at the time but has since been reverted to assert command-span
linkage now that the command span is created before encoding.)

Debugging order if it fails:
1. `maxWireVersion >= 29`? If lower, the driver never attaches the section
   (`NINE_DOT_ZERO_WIRE_VERSION` gate in `CommandMessage`). Beware of another
   local mongod shadowing the port (see Step 2).
2. `featureFlagOtelTraceSampling` currentlyEnabled?
3. Traceparent sampled flag (`-01` suffix) set? Unsampled parents are dropped.
4. mongod log: `BadValue` / `UnknownOpMsgSectionKind` on the telemetry section
   would indicate a wire-format mismatch.
5. Inspect the export files directly (Step 4).

## Step 4: OTLP file export format (verified)

The exporter writes `mongod-<instance>-<yyyymmdd>-trace.jsonl` in the trace
directory: NDJSON, one single-line `{"resourceSpans":[...]}` blob per batch
export. A span's `traceId`, `spanId`, `parentSpanId` (lowercase hex, no
dashes) all appear on the same line, so the test's line-containment matcher
(`serverSpanLinked`) is valid as written. If a future server switches to
pretty-printed JSON, the matcher must parse whole documents instead.

Example exported span:

```json
{"resourceSpans":[{"resource":{...service.name="mongod"...},"scopeSpans":[{"scope":{"name":"mongodb"},
 "spans":[{"name":"find","traceId":"c3778d7ccab7feb73a1e82af44140371",
 "spanId":"100d03d5a6795500","parentSpanId":"461e3f3509da712b","kind":1,"flags":1,...}]}]}]}
```

## Step 5: Clean up

```bash
docker rm -f otel-e2e-mongod
rm -rf /tmp/otel-e2e
```

Keep the downloaded binary (`~/MongoDB/otel-e2e-server/`) for future runs.

---

## Appendix: optional visual verification via Jaeger

For interactive confirmation, point the server at an OTLP HTTP collector
instead of the file exporter:

```bash
docker run --rm -d --name jaeger -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:latest

<mongod> ... \
  --setParameter opentelemetryHttpEndpoint=http://<collector-host>:4318/v1/traces \
  --setParameter featureFlagOtelTraceSampling=true \
  --setParameter 'openTelemetryTracingSampling={defaultSampling: {samplingFactor: 1.0}}'
```

Run the same instrumented `find` and check http://localhost:16686 for a single
trace containing the client operation span, the client command span (a child
of the operation span), and the server span (a child of the command span).

## Notes

- `CommandMessageOtelTraceContextTest` is the regression guard for the
  send-path BSON encoding; `ServerSpanLinkageProseTest` (this runbook) is the
  regression guard for full linkage against a real server.
- Findings (parent-span semantics, tracestate handling, flags) feed back into
  the spec — in particular the operation-vs-command parent-span semantics
  above.
