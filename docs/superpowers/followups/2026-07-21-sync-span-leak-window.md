# Follow-up: sync send-path command-span leak window (pre-existing)

- **For:** future JAVA ticket (not yet filed)
- **Found by:** final code review of the DRIVERS-3454 command-span propagation change (2026-07-21); confirmed
  **pre-existing** — the window is the same size before and after that change, it was not introduced by it.
- **Component:** `driver-core`, `com.mongodb.internal.connection.InternalStreamConnection`, sync path
  `sendAndReceiveInternal` (as of commit `4c5e5dc779`, lines ~463–485).

## The bug

In the sync send path, the command tracing span (a started Micrometer `Observation`) is created, and encode
failures are handled (`InternalStreamConnection.java:456-462` ends the span and rethrows). The *next* failure
handler is the catch around `sendCommandMessage` (`:488-497`), which also ends the span. But the statements
**between** those two protected regions run with a live span and no handler:

1. `message.getCommandDocument(bsonOutput)` hydration (`:469`) — re-parses the encoded message; can throw on a
   BSON invariant violation;
2. `LoggingCommandEventSender` construction + `commandEventSender.sendStartedEvent()` (`:472-476`) — command
   listeners are user code and can throw anything;
3. `tracingSpan.setQueryText(commandDocument)` (`:481`) — serializes the command to (truncated) JSON;
4. `tracingSpan.openScope()` (`:483-485`).

If any of these throws, the exception propagates out of `sendAndReceiveInternal` while the command span is never
`end()`ed (and for #4's window, potentially with an opened-but-never-closed scope).

## Impact

- The command `Observation` never completes → the span never reaches the exporter (or hangs "in flight" in some
  backends), and any registered non-tracing observation handlers (metrics/logging) never see `onStop` — so a
  command that failed in this window is invisible or counted as forever-running in observability data.
- With `TracingObservationHandler`, an unclosed scope from a failed `openScope()` sequence could leak the span
  into the thread's context, corrupting parentage of subsequent unrelated spans on that pooled thread.
- Most likely real-world trigger: a throwing `CommandListener.commandStarted` (user code) with tracing enabled.

Severity is low-ish (requires a throw in a narrow window, and the command fails anyway), but the trace/metrics
corruption mode (scope leak on a pooled thread) justifies a fix.

## Why the async path does not have it

The async equivalent (`:615-704`) wraps the *entire* block — span creation through send initiation — in a single
outer `catch (Throwable)` that ends the span exactly once; after `sendCommandMessageAsync` is invoked, ending is
owned solely by the tracing callback. The sync path evolved differently (try-with-resources + two narrow
catches) and never got whole-block coverage.

## Suggested fix

Restructure the sync path so the span has exactly one owner from creation to hand-off, e.g. wrap everything from
just after `createTracingSpan` up to (and including) the `sendCommandMessage` try in one
`catch (RuntimeException | Error e)` that does `error(e)`, `closeScope()` if the scope was opened, `end()`, and
`commandEventSender.sendFailedEvent(e)` when the sender exists — replacing the current two separate catches
(mirroring the async path's single-owner structure). Care points:

- `closeScope()` must only run if `openScope()` succeeded (track a boolean, or make `closeScope()` idempotent /
  safe before open — check `MicrometerSpan`'s scope implementation).
- Do not double-send `sendFailedEvent` (currently only the `sendCommandMessage` catch sends it; the started event
  fires at `:476`, so a failure before that must not emit a failed event for a never-started command).
- Keep the encode-failure semantics added by the command-span work: encode failures end the span before any scope
  is opened (no `closeScope` on that path).

## Test idea

Unit test with a mock/failing `CommandListener` whose `commandStarted` throws, tracing enabled with
`InMemoryOtelSetup` (or a recording `ObservationRegistry`): assert the command observation is stopped with an
error (finished-span count includes the errored command span) and that no observation scope remains open on the
thread afterwards. A second case: `setQueryText` throwing (e.g. command payload capture enabled with a document
that fails JSON serialization via a hostile codec).

## References

- Final review notes: `.superpowers/sdd/progress.md` (Option A phase) — finding 1.
- Related design: `docs/superpowers/specs/2026-07-18-command-span-propagation-design.md`.
