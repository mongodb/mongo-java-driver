# Follow-up: server span links to the operation span, not the command span

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454)
- **Status:** Known limitation of the POC — fix after the POC is validated.
- **Date:** 2026-06-03
- **Related:** `docs/superpowers/specs/2026-06-01-otel-opmsg-propagation-design.md`,
  `docs/superpowers/specs/2026-06-02-otel-e2e-jaeger-design.md`

## Symptom (observed in Jaeger)

For a single client operation, the exported trace nests as:

```
insert test.ping   (client OPERATION span, id=4d5f339b)
 ├─ insert         (client COMMAND span,  id=d49879c6, parent=4d5f339b)
 └─ insert         (mongod SERVER span,    id=b89795e6, parent=4d5f339b)
```

The `mongod` server span is parented to the client **operation** span, making it a **sibling** of
the client **command** span. The expected/ideal hierarchy is
`operation span → command span → server span` (server span as a child of the command span). Sibling
spans render in Jaeger by start time, which is why the server span can appear "before" the command
span.

## Root cause

The OP_MSG trace-context section is written during message **encoding**, which happens **before** the
per-command span is created. At encode time the only span available in the `OperationContext` is the
operation span, so that is the context propagated on the wire.

Code path (current `nabil_otel_context`):

1. `InternalStreamConnection.sendAndReceiveInternal` (~line 445): `message.encode(bsonOutput, operationContext)`
   runs first. `CommandMessage.writeOtelTraceContextSection` executes inside `encode` and reads
   `operationContext.getTracingSpan()`.
2. `operationContext.getTracingSpan()` returns the **operation** span, set earlier by
   `TracingManager.createOperationSpan` → `operationContext.setTracingSpan(span)`
   (`TracingManager.java` ~line 284).
3. `InternalStreamConnection.sendAndReceiveInternal` (~lines 446–455): the **command** span is created
   **after** encode via `createTracingSpan(...)`, parented to the operation span
   (`TracingManager.java` ~lines 191–192: `addSpan(MONGODB_COMMAND, …, operationSpan.context())`).

So the `traceparent` carries the operation span's id; the server attaches its span to the operation
span; the command span is a separate sibling under the same operation span.

## Why the command span is the better parent

The command span represents an individual wire RPC, and one operation can issue several commands
(retries, `getMore`, split bulk batches). Parenting each server span under the corresponding command
span ties it to the exact RPC that produced it. Under the operation span, multiple server spans pile
up as indistinguishable siblings.

## Why it is not a trivial reorder (chicken-and-egg)

`createTracingSpan` currently derives the command name from `message.getCommandDocument(bsonOutput)`,
i.e. it reads the **already-encoded** bytes. So the command span cannot simply be created before
`encode()` without changing how it obtains the command name.

## Candidate fixes (after the POC)

1. **Create the command span before `encode()` using the in-memory command name.** The command name
   is available from `CommandMessage`'s in-memory command `BsonDocument` (its first key), without
   re-parsing the encoded output. Create the command span first, then have
   `writeOtelTraceContextSection` propagate the command span's context. Requires refactoring
   `createTracingSpan` so the name/parent no longer depend on `getCommandDocument(bsonOutput)`.
   *Preferred — keeps the section inside encoding and yields the correct parent.*

2. **Append the section after the command span is created.** Move section-writing out of
   `CommandMessage.writeOpMsg` into a post-`createTracingSpan` step in `InternalStreamConnection`,
   appending the kind-3 section and re-backpatching the OP_MSG message length. More invasive to wire
   framing; risks interacting with compression and `getCommandDocument` re-parsing.

3. **Accept operation-span parenting for the POC.** The trace is still correctly linked end to end —
   just one level shallower than ideal. No change.

## Decision

Defer. The POC's goal (end-to-end propagation visible in Jaeger) is met with operation-span
parenting. Revisit with option 1 when productionizing, alongside removing the test-only
`OtelTracePropagationTestToggle` and adding the real `hello` `tracingSupport` negotiation.
