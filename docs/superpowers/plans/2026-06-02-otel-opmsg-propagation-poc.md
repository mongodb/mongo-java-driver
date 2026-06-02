# OTel Trace-Context Propagation over OP_MSG (driver-sync POC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the synchronous MongoDB Java driver inject the active client span's W3C `traceparent` into outgoing OP_MSG messages as a new section (kind 3), but only when the server advertised support — validating the wire contract from the DRIVERS-3454 spec.

**Architecture:** The driver already builds OP_MSG sections in `CommandMessage.writeOpMsg()` and creates per-operation Micrometer spans accessible via `OperationContext.getTracingSpan()`. We (1) expose the W3C `traceparent` from the span's `TraceContext`, (2) read a `tracingSupport` capability from the `hello` handshake into `ConnectionDescription` → `MessageSettings`, and (3) write section kind 3 in `writeOpMsg()` gated on both the capability and a non-null sampled traceparent. Validation is phased: automated OP_MSG encode/parse round-trip tests (Phase 1), then an optional manual end-to-end runbook against the server POC (Phase 2).

**Tech Stack:** Java 8 (driver-core baseline), Micrometer Observation (runtime) + Micrometer Tracing (extraction), JUnit 5 + Mockito, Gradle.

**Spec:** `docs/superpowers/specs/2026-06-01-otel-opmsg-propagation-design.md`

> **POC scope guards (do NOT exceed):** sync path only; no reactive/async; no `mongos`/load-balanced propagation; `tracestate` is pass-through only; no sampling rework. All code is internal — no breaking public-API change (only additive withers/getters).

---

## File map

| File | Responsibility | Change |
|---|---|---|
| `driver-core/src/main/com/mongodb/internal/observability/micrometer/TraceContext.java` | Trace context abstraction | Add `@Nullable String traceParent()` |
| `driver-core/src/main/com/mongodb/internal/observability/micrometer/MicrometerTracer.java` | Micrometer impl | Implement `traceParent()` from the `Observation`'s tracing context |
| `driver-core/build.gradle.kts` | Module deps | Add `micrometer-tracing` as `optionalImplementation` |
| `gradle/libs.versions.toml` | Dependency catalog | Add `micrometer-tracing` library coordinate (main) |
| `driver-core/src/main/com/mongodb/internal/connection/DescriptionHelper.java` | hello parsing | Parse `tracingSupport`, set on `ConnectionDescription` |
| `driver-core/src/main/com/mongodb/connection/ConnectionDescription.java` | Connection capabilities | Add `tracingSupport` field, `withTracingSupport()`, `isTracingSupport()` |
| `driver-core/src/main/com/mongodb/internal/connection/MessageSettings.java` | Per-message settings | Add `tracingSupported` field + builder method + getter |
| `driver-core/src/main/com/mongodb/internal/connection/ProtocolHelper.java` | Builds `MessageSettings` | Propagate `tracingSupport` from `ConnectionDescription` |
| `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java` | OP_MSG encoding | Add section kind 3, write it gated on settings + traceparent |
| `docs/superpowers/runbooks/otel-opmsg-e2e.md` | Phase 2 manual validation | New runbook |

---

## Task 1: Expose `traceParent()` on the `TraceContext` abstraction

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/observability/micrometer/TraceContext.java`

- [ ] **Step 1: Add the method to the interface and make `EMPTY` return null**

Replace the interface body so `EMPTY` implements the new method:

```java
@SuppressWarnings("InterfaceIsType")
public interface TraceContext {
    TraceContext EMPTY = new TraceContext() {
        @Override
        public String traceParent() {
            return null;
        }
    };

    /**
     * The W3C {@code traceparent} string for this context
     * ({@code 00-<32hex traceId>-<16hex spanId>-<2hex flags>}),
     * or {@code null} if unavailable or the span is not sampled.
     */
    @Nullable
    String traceParent();
}
```

Add the import `import com.mongodb.lang.Nullable;` below the package statement.

- [ ] **Step 2: Compile to verify the interface change is consistent**

Run: `./gradlew :driver-core:compileJava`
Expected: SUCCESS. (`Span.EMPTY.context()` returns `TraceContext.EMPTY`, which now implements `traceParent()`.) If any other anonymous `TraceContext` implementers exist they will fail to compile — fix each by adding `traceParent()` returning `null`.

- [ ] **Step 3: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/observability/micrometer/TraceContext.java
git commit -m "DRIVERS-3454: add traceParent() to TraceContext"
```

---

## Task 2: Implement `traceParent()` in the Micrometer tracer

The driver only has the Micrometer **Observation** at runtime; the W3C trace/span IDs live in Micrometer **Tracing**, which attaches a `TracingObservationHandler.TracingContext` to the observation's context when a tracing bridge is configured. We read that.

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `driver-core/build.gradle.kts`
- Modify: `driver-core/src/main/com/mongodb/internal/observability/micrometer/MicrometerTracer.java`
- Test: `driver-core/src/test/unit/com/mongodb/internal/observability/micrometer/MicrometerTraceParentTest.java`

- [ ] **Step 1: Add the main `micrometer-tracing` library coordinate**

In `gradle/libs.versions.toml`, under `[libraries]` add (the `micrometer-tracing` version `1.6.0-M3` already exists under `[versions]`):

```toml
micrometer-tracing = { module = "io.micrometer:micrometer-tracing", version.ref = "micrometer-tracing" }
```

- [ ] **Step 2: Add it as an optional dependency of driver-core**

In `driver-core/build.gradle.kts`, directly after line 59 (`optionalImplementation(libs.micrometer.observation)`), add:

```kotlin
optionalImplementation(libs.micrometer.tracing)
```

(The existing `"io.micrometer.*;resolution:=optional"` OSGi import on line 105 already covers the new package.)

- [ ] **Step 3: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/internal/observability/micrometer/MicrometerTraceParentTest.java`:

```java
/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.observability.micrometer;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import com.mongodb.observability.micrometer.MongodbObservation;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicrometerTraceParentTest {
    private static final Pattern TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    @Test
    void returnsTraceParentForSampledSpan() {
        ObservationRegistry registry = ObservationRegistry.create();
        SimpleTracer tracer = new SimpleTracer();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));

        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.COMMAND_OBSERVATION, "find", null, null);
        span.openScope();
        try {
            String traceParent = span.context().traceParent();
            assertNotNull(traceParent);
            assertTrue(TRACEPARENT.matcher(traceParent).matches(), traceParent);
        } finally {
            span.closeScope();
            span.end();
        }
    }

    @Test
    void returnsNullWhenNoTracingBridgeConfigured() {
        ObservationRegistry registry = ObservationRegistry.create();
        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.COMMAND_OBSERVATION, "find", null, null);
        span.openScope();
        try {
            assertNull(span.context().traceParent());
        } finally {
            span.closeScope();
            span.end();
        }
    }
}
```

> Note: confirm the enum constant name in `MongodbObservation` (e.g. `COMMAND_OBSERVATION`); if it differs, use the actual command-span constant. `SimpleTracer`/`DefaultTracingObservationHandler` come from the test-scoped `micrometer-tracing-integration-test` dependency already present.

- [ ] **Step 4: Run the test to verify it fails to compile/fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.observability.micrometer.MicrometerTraceParentTest"`
Expected: FAIL — `MicrometerTraceContext` does not yet implement `traceParent()`.

- [ ] **Step 5: Implement `traceParent()` in `MicrometerTraceContext`**

In `MicrometerTracer.java`, add imports:

```java
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.handler.TracingObservationHandler;
```

> The driver's own type is also named `TraceContext`; reference the Micrometer one by its fully-qualified name in code to avoid the clash (do NOT add the conflicting import). Use `io.micrometer.tracing.TraceContext` inline.

Replace the `MicrometerTraceContext` inner class with:

```java
    /**
     * Represents a Micrometer-based trace context.
     */
    private static class MicrometerTraceContext implements TraceContext {
        @Nullable
        private final Observation observation;

        MicrometerTraceContext(@Nullable final Observation observation) {
            this.observation = observation;
        }

        @Override
        @Nullable
        public String traceParent() {
            if (observation == null) {
                return null;
            }
            TracingObservationHandler.TracingContext tracingContext =
                    observation.getContextView().getOrNull(TracingObservationHandler.TracingContext.class);
            if (tracingContext == null || tracingContext.getSpan() == null) {
                return null;
            }
            io.micrometer.tracing.TraceContext ctx = tracingContext.getSpan().context();
            if (ctx == null || ctx.traceId() == null || ctx.spanId() == null) {
                return null;
            }
            Boolean sampled = ctx.sampled();
            if (sampled == null || !sampled) {
                return null;
            }
            return "00-" + ctx.traceId() + "-" + ctx.spanId() + "-01";
        }
    }
```

> `traceId()`/`spanId()` from Micrometer Tracing are already lowercase hex of the correct length. We emit flags `01` (sampled) because we only propagate sampled spans, matching spec §3.3.

Note: the `import io.micrometer.tracing.TraceContext;` added above is unused if you reference it fully-qualified — remove it and keep only the `TracingObservationHandler` import to satisfy the no-unused-import check.

- [ ] **Step 6: Run the test to verify it passes**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.observability.micrometer.MicrometerTraceParentTest"`
Expected: PASS (both tests).

- [ ] **Step 7: Commit**

```bash
git add gradle/libs.versions.toml driver-core/build.gradle.kts \
  driver-core/src/main/com/mongodb/internal/observability/micrometer/MicrometerTracer.java \
  driver-core/src/test/unit/com/mongodb/internal/observability/micrometer/MicrometerTraceParentTest.java
git commit -m "DRIVERS-3454: extract W3C traceparent from Micrometer span"
```

---

## Task 3: Read `tracingSupport` from `hello` into `ConnectionDescription`

**Files:**
- Modify: `driver-core/src/main/com/mongodb/connection/ConnectionDescription.java`
- Modify: `driver-core/src/main/com/mongodb/internal/connection/DescriptionHelper.java`
- Test: `driver-core/src/test/unit/com/mongodb/internal/connection/DescriptionHelperTracingTest.java`

> `ConnectionDescription` is public API. The change is **additive only** (new wither + getter; existing constructors keep delegating with `tracingSupport=false`), so it is binary-compatible. Do NOT change any existing public constructor signature.

- [ ] **Step 1: Read the actual `ConnectionDescription` constructor chain and the `withServerType` wither**

Run: `sed -n '40,210p' driver-core/src/main/com/mongodb/connection/ConnectionDescription.java`
Note the most-derived constructor (the one with `serviceId` + all fields) and how `withConnectionId`/`withServiceId`/`withServerType` build a copy. You will mirror that pattern exactly.

- [ ] **Step 2: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/internal/connection/DescriptionHelperTracingTest.java`:

```java
/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DescriptionHelperTracingTest {
    private static final ConnectionId CONNECTION_ID =
            new ConnectionId(new ServerId(new com.mongodb.connection.ClusterId(), new ServerAddress()));

    private static BsonDocument hello(final boolean tracing) {
        BsonDocument doc = BsonDocument.parse(
                "{ ok: 1, ismaster: true, maxWireVersion: 25, minWireVersion: 0,"
                        + " maxBsonObjectSize: 16777216, maxMessageSizeBytes: 48000000, maxWriteBatchSize: 100000 }");
        if (tracing) {
            doc.put("tracingSupport", org.bson.BsonBoolean.TRUE);
        }
        return doc;
    }

    @Test
    void parsesTracingSupportTrue() {
        ConnectionDescription description = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, hello(true));
        assertTrue(description.isTracingSupport());
    }

    @Test
    void defaultsTracingSupportFalseWhenAbsent() {
        ConnectionDescription description = DescriptionHelper.createConnectionDescription(
                ClusterConnectionMode.SINGLE, CONNECTION_ID, hello(false));
        assertFalse(description.isTracingSupport());
    }
}
```

> Confirm `createConnectionDescription`'s exact signature/visibility (Explore reported `static ConnectionDescription createConnectionDescription(ClusterConnectionMode, ConnectionId, BsonDocument)`). If the package-private method isn't visible, the test is already in the same `com.mongodb.internal.connection` package, so it is.

- [ ] **Step 3: Run the test to verify it fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.DescriptionHelperTracingTest"`
Expected: FAIL — `isTracingSupport()` does not exist.

- [ ] **Step 4: Add the field, wither, and getter to `ConnectionDescription`**

Add the field next to the other `private final` fields (after `logicalSessionTimeoutMinutes`):

```java
    private final boolean tracingSupport;
```

In the most-derived constructor (the one with `serviceId` and all fields), add `, false` initialization by assigning `this.tracingSupport = false;` at the end of its body. For all other delegating constructors no change is needed (they call the most-derived one).

Add a wither (mirror `withServerType`) — it constructs a copy via the most-derived constructor and then sets the flag. Since the constructor sets `tracingSupport=false`, implement the wither by copying through a private all-args path. Concretely, add:

```java
    /**
     * Returns a copy of this {@code ConnectionDescription} with the given tracing-support capability.
     *
     * @param tracingSupport whether the server advertised OpenTelemetry trace-context support
     * @return the new connection description
     */
    public ConnectionDescription withTracingSupport(final boolean tracingSupport) {
        ConnectionDescription copy = new ConnectionDescription(serviceId, connectionId, maxWireVersion, serverType,
                maxBatchCount, maxDocumentSize, maxMessageSize, compressors, saslSupportedMechanisms,
                logicalSessionTimeoutMinutes);
        copy.tracingSupportOverride = tracingSupport;
        return copy;
    }

    /**
     * @return whether the server advertised OpenTelemetry trace-context support in its hello response
     */
    public boolean isTracingSupport() {
        return tracingSupportOverride != null ? tracingSupportOverride : tracingSupport;
    }
```

Because `tracingSupport` is `final`, add a separate non-final override field to keep the change additive without rewriting every constructor:

```java
    @Nullable
    private Boolean tracingSupportOverride;
```

(`@Nullable` import `com.mongodb.lang.Nullable` already present in this file; verify and add if missing.) Remove the now-unnecessary `this.tracingSupport = false;` line and instead initialize the field inline at declaration: `private final boolean tracingSupport = false;` is illegal for a constructor-set final — so declare it `private final boolean tracingSupport;` and assign `this.tracingSupport = false;` in the most-derived constructor only.

> Rationale: this keeps all five public constructors source- and binary-compatible. The override field carries the capability set post-construction by `withTracingSupport`.

- [ ] **Step 5: Set the capability in `DescriptionHelper.createConnectionDescription`**

Add a parser near the other private getters:

```java
    private static boolean getTracingSupport(final BsonDocument helloResult) {
        return helloResult.getBoolean("tracingSupport", org.bson.BsonBoolean.FALSE).getValue();
    }
```

In `createConnectionDescription`, change the returned value to apply the wither. Find the `return connectionDescription;` (or the `new ConnectionDescription(...)` return) and wrap it:

```java
        return connectionDescription.withTracingSupport(getTracingSupport(helloResult));
```

(If the method returns the `new ConnectionDescription(...)` directly, assign it to a local `ConnectionDescription connectionDescription = new ConnectionDescription(...);` first, then return the wither call.)

- [ ] **Step 6: Run the test to verify it passes**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.DescriptionHelperTracingTest"`
Expected: PASS (both tests).

- [ ] **Step 7: Commit**

```bash
git add driver-core/src/main/com/mongodb/connection/ConnectionDescription.java \
  driver-core/src/main/com/mongodb/internal/connection/DescriptionHelper.java \
  driver-core/src/test/unit/com/mongodb/internal/connection/DescriptionHelperTracingTest.java
git commit -m "DRIVERS-3454: parse hello tracingSupport into ConnectionDescription"
```

---

## Task 4: Carry `tracingSupported` through `MessageSettings`

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/connection/MessageSettings.java`
- Modify: `driver-core/src/main/com/mongodb/internal/connection/ProtocolHelper.java`

- [ ] **Step 1: Add the field + builder method + getter to `MessageSettings`**

Add next to `sessionSupported` (field after line 60):

```java
    private final boolean tracingSupported;
```

In the `Builder` (after `sessionSupported` field, line 82):

```java
        private boolean tracingSupported;
```

Add the builder setter (mirror `sessionSupported(...)`, after line 137):

```java
        public Builder tracingSupported(final boolean tracingSupported) {
            this.tracingSupported = tracingSupported;
            return this;
        }
```

In the private `MessageSettings(final Builder builder)` constructor (line ~197), add:

```java
        this.tracingSupported = builder.tracingSupported;
```

Add the getter (mirror `getMaxWireVersion()`, near line 181):

```java
    public boolean isTracingSupported() {
        return tracingSupported;
    }
```

- [ ] **Step 2: Populate it in `ProtocolHelper`**

In `ProtocolHelper.java` at the `MessageSettings.builder()` chain (line 231), directly after the existing `.sessionSupported(connectionDescription.getLogicalSessionTimeoutMinutes() != null)` line (line 237), add:

```java
                .tracingSupported(connectionDescription.isTracingSupport())
```

- [ ] **Step 3: Compile**

Run: `./gradlew :driver-core:compileJava`
Expected: SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/connection/MessageSettings.java \
  driver-core/src/main/com/mongodb/internal/connection/ProtocolHelper.java
git commit -m "DRIVERS-3454: thread tracingSupported into MessageSettings"
```

---

## Task 5: Write OP_MSG section kind 3 in `CommandMessage.writeOpMsg()`

The section format matches the server POC's `kSecurityToken`/`kOtelTelemetryContext`: a single section-kind byte followed by a CString (no 4-byte length prefix). The server reads it with `readCStr()`.

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java`

- [ ] **Step 1: Add the section-kind constant**

After `PAYLOAD_TYPE_1_DOCUMENT_SEQUENCE` (line 82) add:

```java
    /**
     * Specifies that the `OP_MSG` section payload is a W3C traceparent C-string (OpenTelemetry trace context).
     */
    private static final byte PAYLOAD_TYPE_3_OTEL_TRACE_CONTEXT = 3;
```

- [ ] **Step 2: Add the import for the tracing Span**

In the import block add:

```java
import com.mongodb.internal.observability.micrometer.Span;
```

- [ ] **Step 3: Write the section in `writeOpMsg`, just before the flag bits are backpatched**

In `writeOpMsg(...)`, immediately **before** the comment `// Write the flag bits` (line 280), insert:

```java
        writeOtelTraceContextSection(bsonOutput, operationContext);

```

Then add the private method (place it after `writeOpMsg`, before `writeOpQuery`):

```java
    private void writeOtelTraceContextSection(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext) {
        if (!getSettings().isTracingSupported()) {
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
        bsonOutput.writeByte(PAYLOAD_TYPE_3_OTEL_TRACE_CONTEXT);
        bsonOutput.writeCString(traceParent);
    }
```

> Rationale: at encode time only the operation-level span exists in `OperationContext` (the command span is created later, in `InternalStreamConnection`). The operation span is a valid parent for the server's RPC span. `context().traceParent()` returns `null` for no-op/unsampled spans, so a missing or unsampled span naturally omits the section (spec §3.3). Gating on `isTracingSupported()` ensures we never send the section to a server that would `uassert(40432)` (spec §4).

- [ ] **Step 4: Compile**

Run: `./gradlew :driver-core:compileJava`
Expected: SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java
git commit -m "DRIVERS-3454: write OP_MSG otel trace-context section (kind 3)"
```

---

## Task 6 (Phase 1 validation): OP_MSG round-trip encode test

This is the automated proof of the wire contract: encode a command with a tracing-capable connection + a sampled span, then scan the produced bytes for section kind 3 + the exact traceparent C-string; assert it is absent when the capability is off or the span yields no traceparent.

**Files:**
- Test: `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java`

- [ ] **Step 1: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java`:

```java
/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.ByteBuf;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class CommandMessageOtelTraceContextTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));
    private static final String TRACE_PARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static CommandMessage commandMessage(final boolean tracingSupported) {
        return new CommandMessage(NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_PRIMARY)
                        .sessionSupported(true)
                        .tracingSupported(tracingSupported)
                        .build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, (ServerApi) null);
    }

    private static OperationContext operationContextWithSpan(final String traceParentOrNull) {
        SessionContext sessionContext = mock(SessionContext.class, mock -> {
            when(mock.getClusterTime()).thenReturn(null);
            when(mock.hasSession()).thenReturn(false);
            when(mock.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
            when(mock.notifyMessageSent()).thenReturn(true);
            when(mock.hasActiveTransaction()).thenReturn(false);
            when(mock.isSnapshot()).thenReturn(false);
        });
        TimeoutContext timeoutContext = mock(TimeoutContext.class);
        TraceContext traceContext = () -> traceParentOrNull;
        Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
        return mock(OperationContext.class, mock -> {
            when(mock.getSessionContext()).thenReturn(sessionContext);
            when(mock.getTimeoutContext()).thenReturn(timeoutContext);
            when(mock.getTracingSpan()).thenReturn(span);
        });
    }

    private static byte[] encodeToBytes(final CommandMessage message, final OperationContext operationContext) {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext);
            List<ByteBuf> buffers = output.getByteBuffers();
            byte[] bytes = new byte[output.getSize()];
            int pos = 0;
            for (ByteBuf buf : buffers) {
                int remaining = buf.remaining();
                buf.get(bytes, pos, remaining);
                pos += remaining;
            }
            buffers.forEach(ByteBuf::release);
            return bytes;
        }
    }

    private static boolean containsTraceParentSection(final byte[] message) {
        // Section kind 3 byte immediately followed by the null-terminated traceparent.
        byte[] needle = new byte[1 + TRACE_PARENT.length() + 1];
        needle[0] = 3;
        byte[] tp = TRACE_PARENT.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(tp, 0, needle, 1, tp.length);
        needle[needle.length - 1] = 0;
        outer:
        for (int i = 0; i + needle.length <= message.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (message[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Test
    void writesSectionWhenSupportedAndSampledSpanPresent() {
        byte[] bytes = encodeToBytes(commandMessage(true), operationContextWithSpan(TRACE_PARENT));
        assertTrue(containsTraceParentSection(bytes), "expected OP_MSG section kind 3 with traceparent");
    }

    @Test
    void omitsSectionWhenCapabilityAbsent() {
        byte[] bytes = encodeToBytes(commandMessage(false), operationContextWithSpan(TRACE_PARENT));
        assertFalse(containsTraceParentSection(bytes), "must not send section to non-tracing server");
    }

    @Test
    void omitsSectionWhenSpanHasNoTraceParent() {
        byte[] bytes = encodeToBytes(commandMessage(true), operationContextWithSpan(null));
        assertFalse(containsTraceParentSection(bytes), "must omit section when span yields no traceparent");
    }
}
```

> If `ByteBufferBsonOutput` exposes a different accessor than `getByteBuffers()`/`getSize()`, mirror whatever the existing `CommandMessageTest`/`CommandMessageSpecification` uses to read encoded bytes (e.g. `getByteBuffers()` then `ByteBufNIO`); adjust `encodeToBytes` accordingly. The `TraceContext traceContext = () -> traceParentOrNull;` lambda works because `TraceContext` is now a single-abstract-method interface returning the traceparent.

- [ ] **Step 2: Run to verify it fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.CommandMessageOtelTraceContextTest"`
Expected: FAIL before Task 5 is implemented; after Task 5 it should pass. (If running tasks in order, this confirms PASS.)

- [ ] **Step 3: Run to verify it passes**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.CommandMessageOtelTraceContextTest"`
Expected: PASS (all three).

- [ ] **Step 4: Commit**

```bash
git add driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java
git commit -m "DRIVERS-3454: round-trip test for OP_MSG otel trace-context section"
```

---

## Task 7 (Phase 2, optional/manual): end-to-end runbook against the server POC

Not automated / not CI. Documents how to confirm the server starts a linked child span.

**Files:**
- Create: `docs/superpowers/runbooks/otel-opmsg-e2e.md`

- [ ] **Step 1: Write the runbook**

Create `docs/superpowers/runbooks/otel-opmsg-e2e.md`:

```markdown
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
     only (revert before commit).
4. In Jaeger, confirm a single trace contains BOTH the client `find` span and a server span,
   with the server span's parent = the client span id sent in the traceparent.

## Pass criteria
- One trace, two+ spans, correct parent/child linkage across the client→server boundary.
- With the driver capability off (default), no server span is created (negative check).

## Notes
- This proves the wire format end to end; the automated Phase 1 test
  (`CommandMessageOtelTraceContextTest`) is the regression guard.
- Findings (any format mismatch, tracestate handling, flags) feed back into the spec.
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/runbooks/otel-opmsg-e2e.md
git commit -m "DRIVERS-3454: add Phase 2 e2e validation runbook"
```

---

## Task 8: Full module verification

- [ ] **Step 1: Format + static checks + tests for driver-core**

Run: `./gradlew :driver-core:spotlessApply :driver-core:check`
Expected: BUILD SUCCESSFUL. Fix any spotless/checkstyle issues in the files you touched (copyright headers, import order, no unused imports — particularly the Micrometer `TraceContext` import note in Task 2).

- [ ] **Step 2: Confirm the new tests ran and passed**

Run: `./gradlew :driver-core:test --tests "*OtelTraceContext*" --tests "*MicrometerTraceParent*" --tests "*DescriptionHelperTracing*"`
Expected: PASS.

- [ ] **Step 3: Final commit if spotless changed anything**

```bash
git add -A && git commit -m "DRIVERS-3454: apply spotless formatting" || echo "nothing to commit"
```

---

## Self-Review (completed during planning)

**Spec coverage:**
- §3.1 section kind 3 → Task 5. §3.2 W3C traceparent format → Task 2 (`00-…-01`). §3.3 request-only / sparse / sampled-only → Task 2 (null unless sampled) + Task 5 (omit when null). §4 `tracingSupport` negotiation → Tasks 3–5 (parse → MessageSettings → gate). §6.1 `TraceContext.traceParent()` → Task 1. §6.2 read capability → Task 3. §6.3 inject → Task 5. §6.4 Phase 1 automated → Task 6; Phase 2 manual → Task 7. §7 testing → Tasks 2,3,6 + Task 8 check.
- Out-of-scope items (reactive, mongos, tracestate beyond pass-through, sampling rework) intentionally have no task — matches spec §6.5.

**Known follow-ups (not blockers for the POC):**
- Server `hello` `tracingSupport` flag does not exist yet (SERVER-107128). Phase 1 fully validates the driver without it; Phase 2 documents the temporary override.
- The `ConnectionDescription.tracingSupportOverride` approach (Task 3) keeps public constructors binary-compatible; a production implementation would likely fold the field into a new constructor + builder during the non-POC work.

**Type consistency:** `traceParent()` (Task 1/2/5/6), `isTracingSupport()` on `ConnectionDescription` (Task 3/4), `isTracingSupported()`/`tracingSupported(...)` on `MessageSettings` (Task 4/5/6), `PAYLOAD_TYPE_3_OTEL_TRACE_CONTEXT` (Task 5) — names are used consistently across tasks.
