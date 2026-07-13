# DRIVERS-3454 Reference Implementation Plan — BSON Telemetry Section

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the DRIVERS-3454 POC into the production-quality reference implementation matching the shipped server contract: OP_MSG section kind 3 carrying `{otel: {traceparent: "<55-char W3C>"}}` as BSON, gated on `maxWireVersion >= 29`.

**Architecture:** All changes live in `driver-core` internal packages (shared by sync/reactive). The hello-capability plumbing from the POC is deleted; the gate becomes the existing `MessageSettings.getMaxWireVersion()` compared to a new `ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION = 29`. `MicrometerTracer` is tightened so `TraceContext.traceParent()` never yields a value the server would reject. Prose tests + an orchestration-wired e2e test verify server-span linkage.

**Tech Stack:** Java 8 (driver-core baseline), JUnit 5, Micrometer Tracing test kit (`InMemoryOtelSetup`), Gradle Kotlin DSL.

**Spec:** `docs/superpowers/specs/2026-07-13-otel-telemetry-section-reference-impl-design.md` (read it first).

## Global Constraints

- **LOCAL ONLY: never `git push`.** All commits stay on local branch `nabil_otel_context`.
- Java 8 source baseline in driver-core — no `var`, records, `Stream.toList()`, text blocks, etc.
- No public API changes. Everything under `com.mongodb.internal.*` except the *removal* of the not-yet-released POC additions to `com.mongodb.connection.ConnectionDescription` (added on this branch only, never released — safe to delete).
- Copyright header `Copyright 2008-present MongoDB, Inc.` on new files. No `System.out.println`; SLF4J only.
- Run `./gradlew spotlessApply` before each commit if formatting-sensitive files changed.
- Server contract (from merged 10gen/mongo#56646): section kind 3; payload BSON `{otel: {traceparent: <string>}}`; traceparent exactly 55 chars `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`, non-zero trace-id/span-id, version != `ff`, **no tracestate**; max section 4096 bytes; at most one telemetry section per message.

---

### Task 1: Branch prep — park unrelated noise, merge origin/main

**Files:**
- No source files; git operations only.

**Interfaces:**
- Produces: a working tree at `origin/main`-merged state with only DRIVERS-3454 content, on which all later tasks build.

- [ ] **Step 1: Park unrelated local modifications (do NOT delete)**

The working tree has unrelated noise. Stash tracked modifications with a label; leave untracked files (`prompt.txt`, `bson-kotlin/.../ReproJava6230Test.kt`) in place — they are ignored by later tasks and must not be committed.

```bash
git status --porcelain
# reset the accidentally staged unrelated file, then stash all unrelated tracked changes
git restore --staged bson/src/test/unit/org/bson/BsonDocumentTest.java
git stash push -m "unrelated-local-noise-before-DRIVERS-3454" \
  bson/src/test/unit/org/bson/BsonDocumentTest.java \
  driver-scala/src/integrationTest/scala/tour/QuickTour.scala \
  driver-scala/src/test/scala/org/mongodb/scala/MongoClientSpec.scala \
  mongodb-crypt/build.gradle.kts
```

Expected: `git status --porcelain` shows only the untracked `prompt.txt`, `ReproJava6230Test.kt`, and the followups doc if still staged (`docs/superpowers/followups/...` — commit that separately if staged: `git commit -m "DRIVERS-3454: follow-up notes on mongos version skew" docs/superpowers/followups/2026-06-08-mongos-version-skew-and-negotiation.md`).

- [ ] **Step 2: Merge origin/main**

```bash
git fetch origin
git merge origin/main --no-edit
```

If conflicts arise, they will most likely be in `driver-core` files the POC touched (`ConnectionDescription.java`, `CommandMessage.java`, `gradle/libs.versions.toml`). Resolve keeping BOTH the upstream changes and the POC changes (the POC changes get reworked/deleted in later tasks, so a mechanically correct merge is enough). Commit the merge.

- [ ] **Step 3: Sanity-build driver-core**

```bash
./gradlew :driver-core:compileJava :driver-core:compileTestJava -q
```

Expected: BUILD SUCCESSFUL. If the merge broke compilation, fix and `git commit --amend` the merge resolution.

---

### Task 2: `ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION = 29`

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/operation/ServerVersionHelper.java`

**Interfaces:**
- Produces: `public static final int NINE_DOT_ZERO_WIRE_VERSION = 29;` — consumed by Task 4's gate in `CommandMessage`.

- [ ] **Step 1: Add the constant**

In `ServerVersionHelper`, immediately after `EIGHT_DOT_ZERO_WIRE_VERSION = 25` (line ~33):

```java
    // Server 9.0 (WIRE_VERSION_90 = 29). Minimum wire version for the OP_MSG telemetry
    // section (OTel trace-context propagation, DRIVERS-3454).
    public static final int NINE_DOT_ZERO_WIRE_VERSION = 29;
```

Do NOT change `LATEST_WIRE_VERSION` (it deliberately tracks the newest wire version the driver fully supports).

- [ ] **Step 2: Compile and commit**

```bash
./gradlew :driver-core:compileJava -q
git add driver-core/src/main/com/mongodb/internal/operation/ServerVersionHelper.java
git commit -m "DRIVERS-3454: add NINE_DOT_ZERO_WIRE_VERSION (29) constant"
```

---

### Task 3: Tighten `MicrometerTracer` traceparent production

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/observability/micrometer/MicrometerTracer.java` (inner class `MicrometerTraceContext.traceParent()`, ~line 122)
- Modify: `driver-core/src/main/com/mongodb/internal/observability/micrometer/TraceContext.java` (javadoc only)
- Test: `driver-core/src/test/unit/com/mongodb/internal/observability/micrometer/MicrometerTraceParentTest.java` (exists from POC — rework)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `TraceContext.traceParent()` returns either `null` or a guaranteed-server-valid 55-char string. Behavior change vs POC: **unsampled contexts now return a traceparent with flags `00`** (previously `null`); malformed/zero ids return `null`.

- [ ] **Step 1: Rework the unit test to the new contract**

Replace the POC assertions in `MicrometerTraceParentTest` with a matrix (keep the file's existing test scaffolding/mocks for building Micrometer contexts; adjust to these cases):

```java
    @Test
    void shouldFormatSampledTraceParent() {
        // given a context with traceId "0af7651916cd43dd8448eb211c80319c", spanId "b7ad6b7169203331", sampled=true
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", traceParent);
        assertEquals(55, traceParent.length());
    }

    @Test
    void shouldFormatUnsampledTraceParentWithZeroFlags() {
        // same ids, sampled=false (and also sampled=null)
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00", traceParent);
    }

    @Test
    void shouldReturnNullForInvalidIds() {
        // each of these must yield null:
        // traceId all zeros: "00000000000000000000000000000000"
        // spanId all zeros:  "0000000000000000"
        // traceId wrong length: "abc"
        // spanId wrong length: "abc"
        // traceId uppercase hex: "0AF7651916CD43DD8448EB211C80319C"
        // traceId null / spanId null
        assertNull(traceParent);
    }
```

- [ ] **Step 2: Run to verify the new cases fail**

```bash
./gradlew :driver-core:test --tests 'com.mongodb.internal.observability.micrometer.MicrometerTraceParentTest' -q
```

Expected: FAIL (unsampled currently returns null; invalid ids currently pass through).

- [ ] **Step 3: Implement**

In `MicrometerTracer.MicrometerTraceContext`, replace the body of `traceParent()` from the `io.micrometer.tracing.TraceContext ctx` null-check down, and add the helper:

```java
            io.micrometer.tracing.TraceContext ctx = tracingContext.getSpan().context();
            if (ctx == null) {
                return null;
            }
            String traceId = ctx.traceId();
            String spanId = ctx.spanId();
            if (!isValidNonZeroLowercaseHex(traceId, 32) || !isValidNonZeroLowercaseHex(spanId, 16)) {
                return null;
            }
            Boolean sampled = ctx.sampled();
            return "00-" + traceId + "-" + spanId + (sampled != null && sampled ? "-01" : "-00");
        }

        /**
         * The server ({@code validateW3CTraceparent}) rejects ids that are not exactly the expected
         * length of lowercase hex, or that are all zeroes. Never emit a traceparent it would reject.
         */
        private static boolean isValidNonZeroLowercaseHex(@Nullable final String value, final int expectedLength) {
            if (value == null || value.length() != expectedLength) {
                return false;
            }
            boolean nonZero = false;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                    return false;
                }
                if (c != '0') {
                    nonZero = true;
                }
            }
            return nonZero;
        }
```

Update the javadoc of `TraceContext.traceParent()` to say: returns the 55-char W3C traceparent (`00-<32 hex>-<16 hex>-<flags>`; flags `01` sampled / `00` unsampled), or `null` when there is no valid context (no-op span, missing/zero/malformed ids). Never includes tracestate.

- [ ] **Step 4: Run tests, verify pass**

```bash
./gradlew :driver-core:test --tests 'com.mongodb.internal.observability.micrometer.MicrometerTraceParentTest' -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/observability/micrometer/ driver-core/src/test/unit/com/mongodb/internal/observability/micrometer/
git commit -m "DRIVERS-3454: guarantee traceParent() is server-valid; propagate unsampled with flags 00"
```

---

### Task 4: `CommandMessage` — BSON telemetry section, wire-version gate

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java`
- Test: `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java` (exists from POC — rework)

**Interfaces:**
- Consumes: `ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION` (Task 2); `TraceContext.traceParent()` contract (Task 3); existing `OperationContext.getTracingSpan()` (returns `@Nullable Span`).
- Produces: OP_MSG bytes with trailing section kind 3 whose payload is the BSON document `{otel: {traceparent: <string>}}`. `getCommandDocument()` keeps ignoring the trailing non-sequence section (POC behavior retained).

- [ ] **Step 1: Rework the round-trip test**

Rework `CommandMessageOtelTraceContextTest` (keep its existing harness for encoding a `CommandMessage` and re-reading the produced buffer; the POC test asserted a C-string payload). New assertions:

```java
    @Test
    void shouldWriteTelemetrySectionWhenWireVersionAtLeast29AndSpanActive() {
        // settings: MessageSettings.builder().maxWireVersion(NINE_DOT_ZERO_WIRE_VERSION).build()
        // operationContext with a tracing span whose context().traceParent() returns
        //   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        // after encode(): scan sections after the body; expect one byte 0x03 followed by a BSON document
        BsonDocument telemetry = readTelemetrySectionDocument(buffer);
        assertEquals(new BsonDocument("otel", new BsonDocument("traceparent",
                new BsonString("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"))), telemetry);
    }

    @Test
    void shouldNotWriteTelemetrySectionWhenWireVersionBelow29() {
        // identical setup but maxWireVersion = EIGHT_DOT_ZERO_WIRE_VERSION (25) => no section kind 3
    }

    @Test
    void shouldNotWriteTelemetrySectionWhenNoSpan() {
        // wire version 29, operationContext.getTracingSpan() == null => no section kind 3
    }

    @Test
    void shouldNotWriteTelemetrySectionWhenTraceParentNull() {
        // wire version 29, span present but context().traceParent() == null => no section kind 3
    }

    @Test
    void getCommandDocumentIgnoresTelemetrySection() {
        // encode with section present; CommandMessage.getCommandDocument(...) still returns the command
        // (retains the POC regression test, renamed)
    }

    @Test
    void shouldWriteTelemetrySectionAfterDocumentSequences() {
        // encode a command with a document-sequence payload AND an active span at wire version 29;
        // assert sequence section(s) precede the kind-3 section and the command re-parses correctly
    }
```

`readTelemetrySectionDocument` helper: walk sections after the body exactly like the production parser (kind byte, then for kind 1 skip `int32` size bytes, for kind 3 decode one BSON document via `new BsonDocumentCodec().decode(new BsonBinaryReader(...))`), fail if kind 3 absent.

- [ ] **Step 2: Run to verify failures**

```bash
./gradlew :driver-core:test --tests 'com.mongodb.internal.connection.CommandMessageOtelTraceContextTest' -q
```

Expected: FAIL (payload is still a C-string; gate is still `isTracingSupported()`).

- [ ] **Step 3: Implement in `CommandMessage`**

1. Rename the constant (~line 89) and update its javadoc:

```java
    /**
     * Specifies that the `OP_MSG` section payload is a BSON telemetry document
     * ({@code {otel: {traceparent: <string>}}}). Mirrors the server's {@code kTelemetry = 3}
     * section kind (DRIVERS-3454, 10gen/mongo#56646).
     */
    private static final byte PAYLOAD_TYPE_3_TELEMETRY = 3;
```

2. Replace `writeOtelTraceContextSection` with:

```java
    private void writeTelemetryContextSection(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        if (getSettings().getMaxWireVersion() < NINE_DOT_ZERO_WIRE_VERSION) {
            return;
        }
        Span tracingSpan = operationContext.getTracingSpan();
        if (tracingSpan == null) {
            return;
        }
        String traceParent = tracingSpan.context().traceParent();
        if (traceParent == null) {
            return;
        }
        bsonOutput.writeByte(PAYLOAD_TYPE_3_TELEMETRY);
        BsonDocument telemetry = new BsonDocument("otel",
                new BsonDocument("traceparent", new BsonString(traceParent)));
        new BsonDocumentCodec().encode(new BsonBinaryWriter(bsonOutput), telemetry,
                EncoderContext.builder().build());
    }
```

Update the call site in `writeOpMsg()` (`writeOtelTraceContextSection(...)` → `writeTelemetryContextSection(...)`). Add imports: `org.bson.BsonString`, `org.bson.codecs.BsonDocumentCodec`, `org.bson.codecs.EncoderContext`, `static com.mongodb.internal.operation.ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION`. Remove imports of `OtelTracePropagationTestToggle`. Keep the `getCommandDocument()` handling of trailing non-sequence sections (update its comment to say `PAYLOAD_TYPE_3_TELEMETRY`).

- [ ] **Step 4: Run tests, verify pass**

```bash
./gradlew :driver-core:test --tests 'com.mongodb.internal.connection.CommandMessageOtelTraceContextTest' -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java
git commit -m "DRIVERS-3454: write BSON telemetry section gated on maxWireVersion >= 29"
```

---

### Task 5: Delete POC capability plumbing

**Files:**
- Modify: `driver-core/src/main/com/mongodb/connection/ConnectionDescription.java` (revert POC additions: `tracingSupport` field, constructor param, `withTracingSupport`, `isTracingSupported`, equals/hashCode/toString entries)
- Modify: `driver-core/src/main/com/mongodb/internal/connection/DescriptionHelper.java` (remove `withTracingSupport(...)` call at ~line 72 and `getTracingSupport` helper at ~line 175)
- Modify: `driver-core/src/main/com/mongodb/internal/connection/ProtocolHelper.java` (remove `.tracingSupported(...)` at ~line 238)
- Modify: `driver-core/src/main/com/mongodb/internal/connection/MessageSettings.java` (remove `tracingSupported` field, builder method, getter)
- Delete: `driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java`
- Delete: `driver-core/src/test/unit/com/mongodb/internal/connection/DescriptionHelperTracingTest.java`

**Interfaces:**
- Consumes: Task 4 must be done first (CommandMessage no longer references the toggle or `isTracingSupported()`).
- Produces: clean tree — `git grep -i tracingSupport` and `git grep OtelTracePropagationTestToggle` return nothing.

- [ ] **Step 1: Remove all plumbing**

Use `git diff origin/main -- <file>` on each of the four modified files to see exactly what the POC added, and revert those hunks (the cleanest method: `git checkout origin/main -- driver-core/src/main/com/mongodb/connection/ConnectionDescription.java driver-core/src/main/com/mongodb/internal/connection/DescriptionHelper.java driver-core/src/main/com/mongodb/internal/connection/ProtocolHelper.java driver-core/src/main/com/mongodb/internal/connection/MessageSettings.java` — valid because the POC changes are the ONLY branch changes to these files; verify with `git diff origin/main --stat` first, and re-apply by hand if upstream merge conflicts made the files diverge otherwise). Then:

```bash
git rm driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java
git rm driver-core/src/test/unit/com/mongodb/internal/connection/DescriptionHelperTracingTest.java
```

- [ ] **Step 2: Verify nothing references the removed API**

```bash
git grep -il 'tracingSupport' -- '*.java' ; git grep -l 'OtelTracePropagationTestToggle' -- '*.java'
./gradlew :driver-core:compileJava :driver-core:compileTestJava -q
```

Expected: no grep hits; BUILD SUCCESSFUL. Fix any leftover references (e.g. `DescriptionHelperTest`, `ConnectionDescription` unit tests touched by the POC).

- [ ] **Step 3: Run the affected test suites**

```bash
./gradlew :driver-core:test --tests '*CommandMessage*' --tests '*ConnectionDescription*' --tests '*DescriptionHelper*' --tests '*MessageSettings*' -q
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A driver-core
git commit -m "DRIVERS-3454: remove hello-capability plumbing and test toggle (wire-version gate supersedes)"
```

---

### Task 6: Full driver-core validation

**Files:** none (verification gate).

- [ ] **Step 1: Static checks + full driver-core unit tests**

```bash
./gradlew spotlessApply -q
./gradlew :driver-core:check -q
```

Expected: BUILD SUCCESSFUL. Fix anything that fails (checkstyle on new code, unused imports, etc.).

- [ ] **Step 2: Commit any fixups**

```bash
git status --porcelain  # if formatting changed files:
git add -A && git commit -m "DRIVERS-3454: formatting/static-check fixups"
```

---

### Task 7: Prose-test definitions document

**Files:**
- Create: `docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md`

**Interfaces:**
- Produces: the prose-test text that Task 8's Java test implements (test names must match the prose numbering).

- [ ] **Step 1: Write the document**

Content (verbatim; structured like the existing OTel spec prose tests README so it can be lifted into the future `mongodb/specifications` PR):

```markdown
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
the active span's context.

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
server. Capture the driver's finished command span (client side). Then read the server's
exported OTLP JSON from the trace directory and assert a server span exists whose
`traceId` equals the client command span's trace-id and whose `parentSpanId` equals the
client command span's span-id. Allow for the server's batch export interval when polling.
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md
git commit -m "DRIVERS-3454: prose-test definitions for trace-context propagation"
```

Note: prose tests 1–4 are implemented by `CommandMessageOtelTraceContextTest` (Task 4) and `MicrometerTraceParentTest` (Task 3); add a javadoc line to each test class referencing this document and the prose test numbers.

---

### Task 8: E2E server-span linkage test

**Files:**
- Create: `driver-sync/src/test/functional/com/mongodb/client/observability/ServerSpanLinkageProseTest.java`

**Interfaces:**
- Consumes: `AbstractMicrometerProseTest` patterns (`InMemoryOtelSetup`, `getMongoClientSettingsBuilder()`, `ObservabilitySettings.micrometerBuilder()`), env-var helper `setEnv` if needed for enabling tracing.
- Produces: prose test 5 implementation, gated on system property `org.mongodb.test.otel.trace.dir`.

- [ ] **Step 1: Write the test**

```java
/*
 * Copyright 2008-present MongoDB, Inc.
 * ... (standard Apache header, copy from AbstractMicrometerProseTest)
 */
package com.mongodb.client.observability;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.observability.ObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Prose test 5 (docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md):
 * end-to-end server span linkage via the server's OTLP file exporter.
 *
 * <p>Requires a MongoDB 9.0+ (maxWireVersion >= 29) server on the same host, started with
 * {@code --setParameter opentelemetryTraceDirectory=<dir>} (plus tracing feature flag and
 * sampling parameters), and the test run with
 * {@code -Dorg.mongodb.test.otel.trace.dir=<same dir>}. Skipped otherwise.</p>
 */
@EnabledIfSystemProperty(named = "org.mongodb.test.otel.trace.dir", matches = ".+")
public class ServerSpanLinkageProseTest {

    private static final long EXPORT_POLL_TIMEOUT_MS = 30_000;
    private static final long EXPORT_POLL_INTERVAL_MS = 1_000;

    private final ObservationRegistry observationRegistry = ObservationRegistry.create();
    private InMemoryOtelSetup memoryOtelSetup;
    private InMemoryOtelSetup.Builder.OtelBuildingBlocks inMemoryOtel;

    @BeforeEach
    void setUp() {
        memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
        inMemoryOtel = memoryOtelSetup.getBuildingBlocks();
    }

    @AfterEach
    void tearDown() {
        memoryOtelSetup.close();
    }

    @Test
    @DisplayName("Prose test 5: server emits a child span of the driver command span")
    void testServerSpanLinkage() throws Exception {
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();

        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoCollection<Document> collection =
                    client.getDatabase(getDefaultDatabaseName()).getCollection("serverSpanLinkage");
            collection.find().first();
        }

        List<FinishedSpan> clientSpans = inMemoryOtel.getFinishedSpans();
        assertTrue(clientSpans.size() >= 2, "expected operation + command client spans, got: " + clientSpans);
        // command span is the innermost (first finished) span; see AbstractMicrometerProseTest ordering
        FinishedSpan commandSpan = clientSpans.get(0);
        String traceId = commandSpan.getTraceId();
        String commandSpanId = commandSpan.getSpanId();

        Path traceDir = Paths.get(System.getProperty("org.mongodb.test.otel.trace.dir"));
        long deadline = System.currentTimeMillis() + EXPORT_POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (serverSpanLinked(traceDir, traceId, commandSpanId)) {
                return;
            }
            Thread.sleep(EXPORT_POLL_INTERVAL_MS);
        }
        fail("no server span found in " + traceDir + " with traceId=" + traceId
                + " and parentSpanId=" + commandSpanId);
    }

    /**
     * Scans OTLP JSON export files for a span with the given traceId whose parentSpanId is the
     * driver command span. OTLP file exports contain resourceSpans[].scopeSpans[].spans[] objects
     * with hex-encoded traceId/spanId/parentSpanId fields; a simple containment check on the two
     * hex ids in the same file line is sufficient and avoids a protobuf/JSON-schema dependency.
     */
    private static boolean serverSpanLinked(final Path traceDir, final String traceId, final String parentSpanId)
            throws IOException {
        if (!Files.isDirectory(traceDir)) {
            return false;
        }
        try (Stream<Path> files = Files.walk(traceDir)) {
            List<Path> exportFiles = files.filter(Files::isRegularFile).collect(Collectors.toList());
            for (Path file : exportFiles) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.contains(traceId) && line.contains("\"parentSpanId\":\"" + parentSpanId + "\"")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
```

Adaptation note for the implementer: verify the exact accessor names on `FinishedSpan` (`getTraceId()`/`getSpanId()`) and the client-span ordering against `AbstractMicrometerProseTest` (it asserts `get(0)` is the command span, e.g. `"find"`); also check how that class enables tracing — if tracing is env-var-gated (`ENV_OBSERVABILITY_ENABLED`), reuse its `setEnv` helper in `@BeforeEach`/`@AfterEach` the same way existing tests do. Match whatever compiles against the merged main.

- [ ] **Step 2: Verify compile + skip behavior**

```bash
./gradlew :driver-sync:compileFunctionalJava -q 2>/dev/null || ./gradlew :driver-sync:compileTestJava -q
# run without the property; the test must be SKIPPED, not failed (check the task's test report)
```

Expected: compiles; test skipped when property absent. (Full e2e execution happens in Task 9's runbook flow — it needs the special server.)

- [ ] **Step 3: Commit**

```bash
git add driver-sync/src/test/functional/com/mongodb/client/observability/ServerSpanLinkageProseTest.java
git commit -m "DRIVERS-3454: e2e server-span linkage prose test (OTLP file exporter)"
```

---

### Task 9: Runbook update + manual e2e run

**Files:**
- Modify: `docs/superpowers/runbooks/otel-opmsg-e2e.md`

**Interfaces:**
- Consumes: Task 8's test and its `org.mongodb.test.otel.trace.dir` property.

- [ ] **Step 1: Rewrite the runbook**

Replace its POC content with the current workflow:

1. Download a server 9.0 binary containing 10gen/mongo#56646 (merged 2026-07-06) from Evergreen (any master waterfall compile task after that date; note the artifact URL used).
2. Start it locally, e.g.:

```bash
mkdir -p /tmp/otel-e2e/{db,traces}
<path-to>/mongod --dbpath /tmp/otel-e2e/db --port 27017 \
  --setParameter opentelemetryTraceDirectory=/tmp/otel-e2e/traces \
  --setParameter featureFlagOtelTracing=true \
  --setParameter 'opentelemetrySamplingRates={"default": 1.0}'
```

(Implementer: confirm the exact feature-flag and sampling parameter names from the downloaded binary via `mongod --help` / `getParameter '*'` — they live in `src/mongo/otel/traces/tracing_feature_flags.idl` and `trace_sampling_parameters.idl` on the server; record the working invocation in the runbook.)

3. Run the e2e test:

```bash
./gradlew :driver-sync:test --tests 'com.mongodb.client.observability.ServerSpanLinkageProseTest' \
  -Dorg.mongodb.test.uri="mongodb://localhost:27017" \
  -Dorg.mongodb.test.otel.trace.dir=/tmp/otel-e2e/traces
```

4. Keep the previous Jaeger flow as an optional "visual verification" appendix (`opentelemetryHttpEndpoint=http://localhost:4318/v1/traces`), removing all references to `OtelTracePropagationTestToggle` and `tracingSupport`.

- [ ] **Step 2: Execute the runbook end-to-end**

Actually perform steps 1–3 and record results. Expected: `ServerSpanLinkageProseTest` PASSES against the real server. If the server rejects or ignores the section, debug with `superpowers:systematic-debugging` before touching driver code — likely causes: feature flag name, sampling off, wire version reported < 29, or traceparent validation mismatch.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/runbooks/otel-opmsg-e2e.md
git commit -m "DRIVERS-3454: update e2e runbook for 9.0 server and OTLP file exporter"
```

---

### Task 10: Supersede old spec + final validation

**Files:**
- Modify: `docs/superpowers/specs/2026-06-01-otel-opmsg-propagation-design.md` (status header only)

- [ ] **Step 1: Mark the old design superseded**

Change its `- **Status:**` line to:

```markdown
- **Status:** SUPERSEDED by `2026-07-13-otel-telemetry-section-reference-impl-design.md` (payload is now a BSON document; gating is by maxWireVersion >= 29, not a hello flag)
```

- [ ] **Step 2: Final full validation**

```bash
./gradlew spotlessApply docs check -q
```

Expected: BUILD SUCCESSFUL. (Skip `scalaCheck` unless Scala files were touched; integration tests only with a running server per AGENTS.md.)

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "DRIVERS-3454: mark POC design superseded; final validation fixups"
```

**Do not push. Everything stays local until Nabil says otherwise.**
