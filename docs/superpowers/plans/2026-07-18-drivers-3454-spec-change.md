# DRIVERS-3454 Minimal Spec Change Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Draft the DRIVERS-3454 normative spec change (OP_MSG Payload Type 3 + OTel trace-context propagation) on a local branch of the `mongodb/specifications` submodule.

**Architecture:** Two markdown edits in the spec submodule at `testing/resources/specifications`: the wire-format layer lands in `source/message/OP_MSG.md`, the driver-behavior layer in `source/open-telemetry/open-telemetry.md`. No test files, no tooling regeneration.

**Tech Stack:** GitHub Flavored Markdown (120-char line width), git.

**Spec:** `docs/superpowers/specs/2026-07-18-drivers-3454-spec-change-design.md` (in the parent repo) — read it first.

## Global Constraints

- **LOCAL ONLY: never `git push`**, never update the parent repo's submodule pointer.
- All work happens inside `/Users/nabil.hachicha/MongoDB/mongo-java-driver/nabil_otel_context/testing/resources/specifications` on branch `DRIVERS-3454` based on `origin/master` (repo default; there is no `main`).
- Markdown at **120-character line width**; match each file's existing formatting exactly (note: `OP_MSG.md` uses `### Changelog`, `open-telemetry.md` uses `## Changelog`).
- Changelog entries dated **2026-07-18**, prepended as the first bullet.
- Normative text only: no changes under `tests/`, no Future Work section additions.
- Normative content must match the shipped server contract: payload `{ otel: { traceparent: <string> } }`; gate `maxWireVersion >= 29` (MongoDB 9.0); traceparent exactly 55 chars `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`, non-zero ids; omit-on-invalid; no tracestate; unsampled propagated with flags `00`; at most one section per message; request-only; **operation span** is what's propagated.

---

### Task 1: Branch setup

**Files:** none (git only), all commands run from `testing/resources/specifications`.

**Interfaces:**
- Produces: local branch `DRIVERS-3454` at `origin/master`, on which Tasks 2–3 commit.

- [ ] **Step 1: Fetch and branch**

```bash
cd /Users/nabil.hachicha/MongoDB/mongo-java-driver/nabil_otel_context/testing/resources/specifications
git fetch origin master
git checkout -b DRIVERS-3454 origin/master
git log --oneline -1   # note the base commit in your report
```

Expected: new branch `DRIVERS-3454` checked out, clean status. (The submodule was previously in detached-HEAD state; that is normal. Do NOT touch the parent repo.)

---

### Task 2: `source/message/OP_MSG.md` — Payload Type 3

**Files:**
- Modify: `testing/resources/specifications/source/message/OP_MSG.md` (Section struct ~line 46-55; payload-type rules ~line 74-75; payload-type detail list ~line 168-186; `### Changelog` ~line 406)

**Interfaces:**
- Produces: the OP_MSG-side definition that Task 3's cross-link relies on (anchor: the Payload Type 3 paragraphs added here).

- [ ] **Step 1: Extend the `Section` struct**

In the `struct Section` code block, add one union member after the `sequence` struct:

```c
        document telemetry; // payloadType == 3
```

- [ ] **Step 2: Add the payload-type rules**

Immediately after the paragraph ending "…MUST do so when the batch contains more than one entry." (~line 75), insert:

```markdown

Each `OP_MSG` request MAY additionally contain **at most one** section with `Payload Type 3`, carrying telemetry
context. `Payload Type 3` is request-only: drivers MAY send it, and it MUST NOT appear in server replies. Its payload
is a single BSON document whose contents are defined by the
[OpenTelemetry specification](../open-telemetry/open-telemetry.md). Servers and intermediaries that do not recognize a
section kind fail the message (see below), so senders MUST gate emission of this section on server support as defined
by the OpenTelemetry specification.

> [!NOTE]
> `Payload Type 2` is reserved for server-internal use (a security-token section populated by the server and
> infrastructure components, never by drivers), so the next payload type available for driver-emitted content is 3.
```

(If the file does not use `> [!NOTE]` callouts elsewhere, render the note as a plain paragraph starting with
"Note:" instead — check with `grep -n '\[!NOTE\]' source/message/OP_MSG.md` and match house style.)

- [ ] **Step 3: Add the payload detail entry**

After the "When the Payload Type is 1, the content of the payload is:" block (~line 174-180) and before the "Any
unknown Payload Types MUST result in an error…" paragraph (~line 182), insert:

```markdown
When the Payload Type is 3, the content of the payload is:

- document — a single BSON document containing telemetry context, as defined by the
    [OpenTelemetry specification](../open-telemetry/open-telemetry.md).
```

Also update the sentence at ~line 185 "A fully constructed `OP_MSG` MUST contain exactly one `Payload Type 0`, and
optionally any number of `Payload Type 1`" so it reads:

```markdown
A fully constructed `OP_MSG` MUST contain exactly one `Payload Type 0`, optionally any number of `Payload Type 1`,
and optionally at most one `Payload Type 3`
```

(keeping the remainder of that sentence/paragraph intact).

- [ ] **Step 4: Changelog entry**

Prepend under `### Changelog` (~line 406):

```markdown
- 2026-07-18: Add `Payload Type 3` (telemetry context BSON document; DRIVERS-3454).
```

- [ ] **Step 5: Verify formatting and commit**

```bash
awk 'length > 120 {print FILENAME": "NR" ("length")"}' source/message/OP_MSG.md   # expect no output
git add source/message/OP_MSG.md
git commit -m "DRIVERS-3454: define OP_MSG Payload Type 3 (telemetry context)"
```

---

### Task 3: `source/open-telemetry/open-telemetry.md` — propagation behavior

**Files:**
- Modify: `testing/resources/specifications/source/open-telemetry/open-telemetry.md` (new `####` subsection at the end of Implementation Requirements, i.e. after the `##### Exceptions` block that precedes `## Motivation for Change` ~line 325; Design Rationale ~line 415; `## Changelog` ~line 426)

**Interfaces:**
- Consumes: Task 2's OP_MSG Payload Type 3 definition (cross-link `../message/OP_MSG.md`).

- [ ] **Step 1: Add the propagation subsection**

Insert immediately before `## Motivation for Change` (so it is the last `####` inside Implementation Requirements):

```markdown
#### Propagating Trace Context to the Server

Drivers MUST propagate the active trace context to servers that support it, so that server-generated spans join the
same distributed trace as the driver's spans.

The trace context is carried in an `OP_MSG` section with
[`Payload Type 3`](../message/OP_MSG.md), whose payload is a single BSON document with the following schema:

```json
{ "otel": { "traceparent": "<string>" } }
```

`traceparent` is the [W3C traceparent](https://www.w3.org/TR/trace-context/#traceparent-header) value of the
**operation span**. Drivers attach the trace context when encoding the wire message, before the command span is
created; server spans therefore join the trace as children of the operation span.

Drivers MUST attach the section to a command if and only if all of the following hold:

1. Tracing is enabled on the `MongoClient`.
2. The connection's `maxWireVersion` is greater than or equal to 29 (MongoDB 9.0).
3. A valid `traceparent` value is available from the active operation span.

A `traceparent` value is valid if and only if it is exactly 55 characters of the form
`00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>` and neither the trace-id nor the parent-id is all zeroes.
This mirrors the server-side validation. If no valid value is available, drivers MUST omit the section entirely
rather than send an invalid or truncated value. Drivers MUST NOT append a `tracestate` suffix. Unsampled trace
contexts are propagated with trace-flags `00`.

A message MUST NOT contain more than one telemetry section. Connections that carry no operation span (for example
server monitoring and authentication) naturally send no section, since condition 3 cannot hold.

No tracing data is returned in server responses as part of this feature.
```

Note on the nested code fence: the JSON block inside this section uses a standard triple-backtick fence; when
inserting, make sure the surrounding document structure stays valid (the snippet above shows the intended rendered
content — reproduce it with correct fencing in the file).

- [ ] **Step 2: Add Design Rationale entries**

After the `### No URI options` block (before `## Changelog`), insert:

```markdown
### Wire protocol version gate instead of a hello capability

An alternative was for servers to advertise support via a `hello` response field. Gating on `maxWireVersion` was
chosen instead: the wire protocol version acts as the schema contract for the telemetry payload, so drivers know
exactly which fields a server accepts, and any future payload additions require a wire version bump rather than a
second, parallel negotiation mechanism.

### BSON document instead of a bare traceparent string

Carrying the traceparent inside a BSON document allows future propagation fields to be added to the same section
without redesigning the payload format.
```

- [ ] **Step 3: Changelog entry**

Prepend under `## Changelog`:

```markdown
- 2026-07-18: Add trace context propagation to the server via the `OP_MSG` telemetry section (DRIVERS-3454).
```

- [ ] **Step 4: Verify formatting and commit**

```bash
awk 'length > 120 {print FILENAME": "NR" ("length")"}' source/open-telemetry/open-telemetry.md   # expect no output
git add source/open-telemetry/open-telemetry.md
git commit -m "DRIVERS-3454: specify trace context propagation to the server"
```

---

### Task 4: Repo checks

**Files:** none.

- [ ] **Step 1: Run available repo tooling**

```bash
cd /Users/nabil.hachicha/MongoDB/mongo-java-driver/nabil_otel_context/testing/resources/specifications
command -v pre-commit && pre-commit run --files source/message/OP_MSG.md source/open-telemetry/open-telemetry.md || echo "pre-commit unavailable"
command -v mkdocs && mkdocs build --strict 2>&1 | tail -5 || echo "mkdocs unavailable"
```

If a hook rewrites formatting, review the diff, re-verify content is intact, and amend the relevant commit. If
tooling is unavailable, note that in the report; the `awk` width checks from Tasks 2-3 plus a markdown render
sanity-check (e.g. `grep -c '^#### Propagating'`) suffice.

- [ ] **Step 2: Final state check**

```bash
git log --oneline origin/master..HEAD    # expect exactly the 2 commits from Tasks 2-3
git status --porcelain                   # expect clean
cd /Users/nabil.hachicha/MongoDB/mongo-java-driver/nabil_otel_context && git status --porcelain -- testing/resources/specifications
```

The parent repo will show the submodule as modified (new HEAD); do NOT commit that pointer change — leave it, and
say so in the report.

**Do not push anything. Everything stays local.**
