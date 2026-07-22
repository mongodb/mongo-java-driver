# Follow-up: sync send-path command-span leak window (pre-existing)

- **Status: IMPLEMENTED 2026-07-22** in commit `b4c8e8caf3` (single-owner `catch (Throwable)` in the sync path,
  `LoggingCommandEventSender` started-event pairing guard [option C], `Span.closeScope()` no-op contract javadoc).
  Verified: `LoggingCommandEventSenderSpecification` (incl. new pairing test), `InternalStreamConnection*`,
  `CommandMessageOtelTraceContextTest`, micrometer suites (124 tests green), and `MicrometerProseTest` against a
  live wire-29 server. A JAVA ticket should still be filed retroactively for release notes; the commit message
  carries a `JAVA-XXXX` placeholder to amend once the ticket exists.
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

Restructure the sync path so the span has exactly one owner from creation to hand-off: wrap everything from just
after `createTracingSpan` up to (and including) `sendCommandMessage` in one `catch (RuntimeException | Error e)`
that does `error(e)`, `closeScope()`, `end()`, and `commandEventSender.sendFailedEvent(e)` — replacing the
current two separate catches (mirroring the async path's single-owner structure). This also removes the
duplicated error/end snippets between the encode catch and the send catch.

The unified catch can call `closeScope()` unconditionally — no "was the scope opened" flag is needed (reviewed
2026-07-22): `MicrometerSpan.closeScope()` null-guards its `scope` field and nulls it after closing, so calling
it before `openScope()` or twice is a no-op, and `Span.EMPTY` no-ops trivially. Care points:

- **Promote that no-op behavior from implementation detail to interface contract**: add to
  `Span.closeScope()`'s javadoc — "Calling this without a prior `openScope()`, or more than once, is a no-op." —
  so the unified error path (and any future `Span` implementation or test double) can rely on it, and so a later
  refactor doesn't "simplify away" the null-guard. Note `openScope()`'s current javadoc ("Must be paired with
  {@link #closeScope()} in a try-finally block") reads stricter than this usage; adjust wording accordingly.
- Do not double-send `sendFailedEvent` (currently only the `sendCommandMessage` catch sends it; the started event
  fires at `:476`). Initialize `commandEventSender` to `NoOpCommandEventSender` before the block so failures
  prior to sender creation no-op. New edge to decide consciously: if `sendStartedEvent` itself throws, the
  logging sender is already assigned and a failed event would be emitted for a command whose started event never
  completed — arguably more correct than today's silence, but it is a command-monitoring behavior change.
- Unify the exception types on `catch (Throwable t)` + `throw t;`, matching the async path (decided 2026-07-22).
  This compiles without signature changes via Java 7 precise rethrow (the catch parameter is effectively final
  and the try body declares no checked exceptions — the same rule the current `catch (Exception) { throw e; }`
  already relies on), fixes the current send catch's `Error` blind spot, and forces a compile error if a future
  edit introduces a checked-exception throw site. Trade-off (accepted, same as async): fatal errors like OOM run
  best-effort cleanup before propagating.
- Keep the encode-failure semantics added by the command-span work: an encode failure ends the span before any
  scope was opened — with unconditional `closeScope()` this holds automatically per the no-op contract above.

## Test idea

> Note (2026-07-22): the throwing-`CommandListener` variant below is NOT viable — user listener exceptions are
> already swallowed in `ProtocolHelper.sendCommandStartedEvent` (`catch (Exception)` + warn log), so they cannot
> reach this window. The implemented test instead unit-tests the sender's pairing guard directly
> (`LoggingCommandEventSenderSpecification`: terminal events without a completed started event are suppressed).

Unit test with a mock/failing `CommandListener` whose `commandStarted` throws, tracing enabled with
`InMemoryOtelSetup` (or a recording `ObservationRegistry`): assert the command observation is stopped with an
error (finished-span count includes the errored command span) and that no observation scope remains open on the
thread afterwards. A second case: `setQueryText` throwing (e.g. command payload capture enabled with a document
that fails JSON serialization via a hostile codec).

## References

- Final review notes: `.superpowers/sdd/progress.md` (Option A phase) — finding 1.
- Related design: `docs/superpowers/specs/2026-07-18-command-span-propagation-design.md`.
