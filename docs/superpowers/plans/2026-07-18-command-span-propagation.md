# Command-Span Trace Propagation Implementation Plan (Option A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Propagate the traceparent of the **command** span (not the operation span) in the OP_MSG telemetry section, by creating the command span before message encoding.

**Architecture:** `TracingManager.createTracingSpan` stops depending on encoded bytes (it reads the raw command document that `CommandMessage` already holds); `InternalStreamConnection` hoists span creation above `encode()` in both sync and async paths; `CommandMessage` gains an `encode` overload carrying the command span, consumed by `writeTelemetryContextSection`. Spec branch, prose docs, and e2e assertions flip from operation-span to command-span parentage.

**Tech Stack:** Java 8 (driver-core), JUnit 5, GFM specs.

**Spec:** `docs/superpowers/specs/2026-07-18-command-span-propagation-design.md` — read it first.

## Global Constraints

- **LOCAL ONLY: never `git push`** (parent repo branch `nabil_otel_context`; spec submodule branch `DRIVERS-3454`).
- Java 8 source; no public API changes; everything under `com.mongodb.internal.*`.
- `docs/superpowers/` is gitignored in the parent repo — use `git add -f` for docs.
- Do not change `RequestMessage.encode(ByteBufferBsonOutput, OperationContext)`'s signature (other message types use it).
- The operation span REMAINS the command span's parent (`TracingManager.createTracingSpan` line ~191-192) — only the *propagated* context changes.
- Behavior contract after this change: telemetry section carries the command span's traceparent; null command span (tracing disabled / sensitive command / no-span paths) ⇒ **no section**, even if an operation span exists.

---

### Task 1: `TracingManager.createTracingSpan` reads the raw command document

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/observability/micrometer/TracingManager.java:174-186`
- Modify: `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java` (add package-private raw-command accessor)
- Modify: `driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java:448-455, 624-631` (call sites — argument change only in this task; reorder happens in Task 3)

**Interfaces:**
- Produces: `public Span createTracingSpan(CommandMessage message, OperationContext operationContext, Predicate<String> isSensitiveCommand, Supplier<ServerAddress> serverAddressSupplier, Supplier<ConnectionId> connectionIdSupplier)` — the `Supplier<BsonDocument> commandDocumentSupplier` parameter is REMOVED; the method reads `message.getRawCommandDocument()`.
- Produces: `CommandMessage`: `BsonDocument getRawCommandDocument()` (package-private is impossible across packages — TracingManager is in a different package, so make it `public` on the internal class, javadoc'd as internal) returning the `command` field (`CommandMessage.java:96`).

- [ ] **Step 1: Add the accessor to `CommandMessage`**

Next to the other getters:

```java
    /**
     * The raw (pre-encoding) command document this message was constructed with. Unlike
     * {@link #getCommandDocument(ByteBufferBsonOutput)}, this does not include fields added during encoding
     * ({@code $db}, {@code $readPreference}) nor document-sequence fields, but its first key is always the
     * command name, and it is available before {@code encode()} runs.
     */
    public BsonDocument getRawCommandDocument() {
        return command;
    }
```

- [ ] **Step 2: Change `createTracingSpan`**

In `TracingManager.java`, remove the `commandDocumentSupplier` parameter and replace its use:

```java
    public Span createTracingSpan(final CommandMessage message,
            final OperationContext operationContext,
            final Predicate<String> isSensitiveCommand,
            final Supplier<ServerAddress> serverAddressSupplier,
            final Supplier<ConnectionId> connectionIdSupplier
            ) {

       if (!isEnabled()) {
            return null;
        }
        BsonDocument command = message.getRawCommandDocument();
        String commandName = command.getFirstKey();
        ...
```

The rest of the body is unchanged — `command.containsKey("getMore")` / `command.getInt64("getMore")` now read the raw document, where the `getMore` field is identically present.

- [ ] **Step 3: Update the two call sites in `InternalStreamConnection` (drop the supplier argument only)**

At `:448` and `:624`, delete the `() -> message.getCommandDocument(bsonOutput),` argument line. Do NOT move the calls yet (Task 3).

- [ ] **Step 4: Compile and run tracing tests**

Run: `./gradlew :driver-core:compileJava :driver-core:test --tests 'com.mongodb.internal.observability.micrometer.*' -q`
Expected: BUILD SUCCESSFUL, tests pass.

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main
git commit -m "DRIVERS-3454: createTracingSpan reads the raw command document (no encoded-bytes dependency)"
```

---

### Task 2: `CommandMessage.encode` overload carrying the command span

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java:250, 295, 302-315`
- Test: `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java`

**Interfaces:**
- Consumes: existing `RequestMessage.encode(ByteBufferBsonOutput, OperationContext)` (untouched).
- Produces: `public void encode(ByteBufferBsonOutput bsonOutput, OperationContext operationContext, @Nullable Span commandSpan)` on `CommandMessage`. The 2-arg `encode` inherited from `RequestMessage` now results in NO telemetry section (span defaults to null).

- [ ] **Step 1: Rework the unit test to the new contract (TDD)**

In `CommandMessageOtelTraceContextTest`, change every `message.encode(output, operationContext)` call in the positive-path tests to `message.encode(output, operationContext, span)`, and repurpose `buildOperationContext(span)` → `buildOperationContext()` (the operation context no longer needs to carry the span for these tests; keep the method if other setup depends on it, passing null). Update the test matrix:

```java
    // Prose test 1 — section present: encode(output, operationContext, span) with wire version 29 => kind-3 section
    //   whose traceparent matches the span passed to encode (the COMMAND span).
    // Prose test 2 — wire version 25 + span passed => no section.
    // Prose test 3 — encode(output, operationContext, null) at wire version 29 => no section
    //   (rename shouldNotWriteTelemetrySectionWhenNoSpan; note: this now also covers "operation span exists but
    //   command span is null", since the operation context's span is irrelevant to the section).
    // Prose test 4 — span whose context().traceParent() returns null => no section.
    // getCommandDocumentIgnoresTelemetrySection — unchanged semantics, 3-arg encode with span.
    // shouldWriteTelemetrySectionAfterDocumentSequences — 3-arg encode with span.
    // NEW: shouldNotWriteTelemetrySectionViaTwoArgEncode — message.encode(output, operationContext) (2-arg)
    //   never writes a section even at wire version 29 (guards monitoring/compression/other callers).
```

- [ ] **Step 2: Run to verify failures**

Run: `./gradlew :driver-core:test --tests 'com.mongodb.internal.connection.CommandMessageOtelTraceContextTest' -q`
Expected: FAIL (no 3-arg encode exists).

- [ ] **Step 3: Implement in `CommandMessage`**

Add a transient field + overload (a `CommandMessage` is encoded at most once per send attempt, single-threaded, so a field set for the duration of encode is safe; the overload documents that):

```java
    @Nullable
    private Span commandSpanForEncoding;

    /**
     * Encodes the message, attaching the OP_MSG telemetry section with {@code commandSpan}'s trace context when
     * the gating conditions hold (see writeTelemetryContextSection). The 2-arg {@code encode} never attaches the
     * section. Not thread-safe: a message must be encoded by one thread at a time (already the case — a
     * CommandMessage is built and encoded per send attempt).
     */
    public void encode(final ByteBufferBsonOutput bsonOutput, final OperationContext operationContext,
            @Nullable final Span commandSpan) {
        this.commandSpanForEncoding = commandSpan;
        try {
            encode(bsonOutput, operationContext);
        } finally {
            this.commandSpanForEncoding = null;
        }
    }
```

Change `writeTelemetryContextSection` (drop the `operationContext` parameter — no longer used; update its call at `:295`):

```java
    private void writeTelemetryContextSection(final ByteBufferBsonOutput bsonOutput) {
        if (getSettings().getMaxWireVersion() < NINE_DOT_ZERO_WIRE_VERSION) {
            return;
        }
        Span commandSpan = this.commandSpanForEncoding;
        if (commandSpan == null) {
            return;
        }
        String traceParent = commandSpan.context().traceParent();
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

Remove the now-unused `operationContext.getTracingSpan()` read here (do NOT remove `OperationContext.getTracingSpan()` itself — `TracingManager` uses it for parenting).

- [ ] **Step 4: Run tests, verify pass**

Run: `./gradlew :driver-core:test --tests 'com.mongodb.internal.connection.CommandMessageOtelTraceContextTest' -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java
git commit -m "DRIVERS-3454: encode overload carries the command span for the telemetry section"
```

---

### Task 3: Hoist span creation above encode in `InternalStreamConnection`

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java` sync `sendAndReceiveInternal` (~`:441-480`) and async equivalent (~`:615-650`). The one-way `send` path (`:508`) and compression paths (`:540`, `:681`) keep the 2-arg encode (no command span there today).

**Interfaces:**
- Consumes: Task 1's `createTracingSpan` (no supplier param); Task 2's `encode(bsonOutput, operationContext, commandSpan)`.

- [ ] **Step 1: Reorder the sync path**

Current shape (`:443-455`): `encode(...)` then `tracingSpan = createTracingSpan(...)`. New shape:

```java
        CommandEventSender commandEventSender;
        Span tracingSpan;
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this)) {
            tracingSpan = operationContext
                    .getTracingManager()
                    .createTracingSpan(message,
                            operationContext,
                            cmdName -> SECURITY_SENSITIVE_COMMANDS.contains(cmdName)
                                    || SECURITY_SENSITIVE_HELLO_COMMANDS.contains(cmdName),
                            () -> getDescription().getServerAddress(),
                            () -> getDescription().getConnectionId()
                    );
            try {
                message.encode(bsonOutput, operationContext, tracingSpan);
            } catch (RuntimeException | Error e) {
                if (tracingSpan != null) {
                    tracingSpan.error(e);
                    tracingSpan.end();
                }
                throw e;
            }
            ...rest unchanged (isLoggingCommandNeeded, getCommandDocument hydration, setQueryText, openScope)...
```

The existing failure handling after this point (`:484-486` `error/closeScope/end`) is unchanged — the new catch only covers the window where the span exists but the established handlers don't yet.

- [ ] **Step 2: Reorder the async path identically**

Same transformation at ~`:618-631` — `tracingSpan = createTracingSpan(...)` first, then `message.encode(bsonOutput, operationContext, tracingSpan)` inside a try/catch that calls `tracingSpan.error(e); tracingSpan.end();` before rethrowing/propagating through the existing async error handling (match how that block reports errors — it already has a `try` whose catch handles cleanup; ensure the span ends exactly once: if the surrounding catch already ends the span when non-null, rely on it instead of a nested catch — inspect and pick the single-end structure that fits the existing block).

- [ ] **Step 3: Run driver-core connection + tracing tests**

Run: `./gradlew :driver-core:test --tests 'com.mongodb.internal.connection.*' --tests 'com.mongodb.internal.observability.micrometer.*' -q`
Expected: PASS.

- [ ] **Step 4: Prose tests against local server (functional)**

Requires a running MongoDB at localhost:27017. Run:
`./gradlew :driver-sync:test --tests 'com.mongodb.client.observability.MicrometerProseTest' -Dorg.mongodb.test.uri="mongodb://localhost:27017" -q`
Expected: PASS (span counts/names unchanged — only creation timing moved).

- [ ] **Step 5: Commit**

```bash
git add driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java
git commit -m "DRIVERS-3454: create command span before encode; propagate its context"
```

---

### Task 4: Flip e2e + prose docs to command-span parentage

**Files:**
- Modify: `driver-sync/src/test/functional/com/mongodb/client/observability/ServerSpanLinkageProseTest.java` (assertion comments/javadoc — the span it selects, `getFinishedSpans().get(0)` named e.g. "find", IS the command span per `AbstractMicrometerProseTest` ordering; verify and update the javadoc/comments that currently say "operation span", and any variable names like `operationSpanId`)
- Modify: `docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md` (tests 1 and 5: trace-id/span-id of the **command span**)
- Modify: `docs/superpowers/runbooks/otel-opmsg-e2e.md` (any operation-span wording)

**Interfaces:**
- Consumes: Tasks 1-3 (behavior now propagates the command span).

- [ ] **Step 1: Update the e2e test's span selection and wording**

Read the test first. If it currently picks the span by name "find" via `get(0)` — that IS the command span (`AbstractMicrometerProseTest` asserts `get(0).getName() == "find"` = command span, `get(1)` = operation span "find db.test"); then the previous operation-span behavior was asserted via `getParentId()` or similar adaptation made in the earlier fix — align the selection so the asserted parent is the command span's own span-id, and fix all comments/javadoc that explain operation-span semantics.

- [ ] **Step 2: Update prose-test definitions and runbook wording**

In `2026-07-13-otel-telemetry-section-prose-tests.md`: test 1's "trace-id/span-id match the active span's context" → "match the **command span**'s context"; test 5's "parentSpanId equals the client command span's span-id" (already command-oriented from the original draft — make sure any 2026-07-14-era rewording to "operation span" is reverted to command span). Same sweep in the runbook.

- [ ] **Step 3: Run the e2e test against the local 9.0 server (the real proof)**

Start the wire-29 server (from the runbook; binary at `~/MongoDB/otel-e2e-server/dist-test`, Docker image `otel-poc-mongod:latest`) with the OTLP file exporter + `featureFlagOtelTraceSampling=true` + `openTelemetryTracingSampling={defaultSampling: {samplingFactor: 1.0}}` and a trace directory volume-mounted to the host. Then:

```bash
./gradlew :driver-sync:test --tests 'com.mongodb.client.observability.ServerSpanLinkageProseTest' \
  -Dorg.mongodb.test.uri="mongodb://localhost:27017" \
  -Dorg.mongodb.test.otel.trace.dir=<host trace dir> --rerun
```

Expected: PASS with the server span's `parentSpanId` == the client **command** span id. Stop and remove the container afterwards.

- [ ] **Step 4: Commit**

```bash
git add -f driver-sync/src/test/functional/com/mongodb/client/observability/ServerSpanLinkageProseTest.java docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md docs/superpowers/runbooks/otel-opmsg-e2e.md
git commit -m "DRIVERS-3454: e2e + prose docs assert command-span parentage"
```

---

### Task 5: Flip the spec branch to command span

**Files:**
- Modify: `testing/resources/specifications/source/open-telemetry/open-telemetry.md` (branch `DRIVERS-3454` in the submodule)

**Interfaces:**
- Consumes: nothing from other tasks (text-only), but merge AFTER Task 3 proves the behavior.

- [ ] **Step 1: Amend the propagation subsection**

On submodule branch `DRIVERS-3454` (verify `git branch --show-current`), in the "Propagating Trace Context to the Server" section, replace the operation-span paragraph and condition 3:

Current text (post-review): "`traceparent` is the ... value of the **operation span**: the propagated context MUST be that of the operation span, not the command span; server spans therefore join the trace as children of the operation span. (In practice, drivers attach the trace context when encoding the wire message, before the command span is created.)" and "3. A valid `traceparent` value is available from the active operation span."

New text:

```markdown
`traceparent` is the [W3C traceparent](https://www.w3.org/TR/trace-context/#traceparent-header) value of the **command
span**: the propagated context MUST be that of the command span for the command being sent, so server spans join the
trace as children of the exact command (and retry attempt) that produced them.
```

and condition 3: "3. A valid `traceparent` value is available from the command span for the command being sent." Also update the monitoring/auth sentence if it references "operation span" ("Connections that carry no operation span" → "Commands that carry no command span (for example server monitoring, authentication, and security-sensitive commands) naturally send no section, since condition 3 cannot hold.").

- [ ] **Step 2: Amend the OTel-spec commit and verify hooks**

```bash
cd testing/resources/specifications
git add source/open-telemetry/open-telemetry.md
git commit --amend --no-edit    # amends "DRIVERS-3454: specify trace context propagation to the server"
pre-commit run --files source/open-telemetry/open-telemetry.md
```

Expected: hooks pass (if mdformat rewrites, re-add and re-amend until clean). If the OTel commit is not HEAD, use the reset-soft rebuild pattern from the ledger instead of amend. NOTE: the user's fork branch `nabil/nh/otel/DRIVERS-3454` will need another force push (their call — do not push).

- [ ] **Step 3: Update the parent-repo design docs**

In the parent repo: `docs/superpowers/specs/2026-07-13-otel-telemetry-section-reference-impl-design.md` — add one line under §3.1 noting the 2026-07-18 amendment (command span propagated; see `2026-07-18-command-span-propagation-design.md`). Commit with `git add -f`.

```bash
git add -f docs/superpowers/specs/2026-07-13-otel-telemetry-section-reference-impl-design.md
git commit -m "DRIVERS-3454: note command-span amendment in reference-impl design"
```

---

### Task 6: Full validation

**Files:** none.

- [ ] **Step 1: Full driver-core check (no concurrent Gradle builds!)**

```bash
./gradlew spotlessApply :driver-core:check -q
```

Expected: BUILD SUCCESSFUL, except the known pre-existing environmental failure `TypeMqlValuesFunctionalTest.asStringTestNested` (fails identically on origin/main vs the local server — ignore it; anything else failing must be investigated).

- [ ] **Step 2: Commit any spotless fixups**

```bash
git status --porcelain   # if formatting changed files:
git add -A driver-core && git commit -m "DRIVERS-3454: formatting fixups"
```
