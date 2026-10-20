# PR-2: async framework debt + generator scaffolding

Notes for reviewers; see commit messages for per-file detail.

## What this PR does
- Marker annotations in new package `com.mongodb.internal.annotations`
  (`@HasAsync @GeneratedAsync @SyncLeaf @AsyncLeaf
  @CallbackBoundary @DeadSync @DeadAsync @NoAsync`). Documentation only, no
  runtime use. `@HasAsync` is placed by the generator (its model walks from
  the leaf pairs to their callers), never by hand. `@NoAsync`: placed by hand
  on a sync method that is sync only by design although the model reaches it
  (its async half only throws); the generator never writes or removes it.
- `errorHandlingCallback` moved to the 4 `@CallbackBoundary` crossings
  (`OperationExecutorImpl` x2, `BatchCursor.next`, `CryptConnection.commandAsync`)
  and removed from every internal async method (22 files).
- Leaf annotations on the Stream and Connection command pairs, and on the
  hand-written pairs that already exist and whose async form cannot come from
  a sync body (wait queue, `doAsSubject`, retry loop, resource helpers):
  `RetryControl.doWhileDisabled`, the five `SyncOperationHelper` /
  `AsyncOperationHelper` resource and retry helpers,
  `SingleBatchCursor.createEmptySingleBatchCursor`, `DefaultConnectionPool.get`,
  `OpenConcurrencyLimiter.openWithConcurrencyLimit`,
  `SaslAuthenticator.authenticate`, `ConnectionFactory.create`.
- `@NoAsync` on the two sync-only operations, `LegacyMixedBulkWriteOperation`
  and `MapReduceIterableImpl.WrappedMapReduceReadOperation` (`execute`).

## Decisions
- `errorHandlingCallback` is a net for a callback that THROWS (log + swallow).
  Inside the driver, callbacks are ours and must not throw; a throw is a bug,
  and the per-method wraps hid it (logged, then the chain stalled). At the
  boundary the callback wraps a Reactor sink, and user `Subscriber` code can
  throw through it, so that one net stays.
  - Wrapping is idempotent (an already-wrapped callback is returned as-is), so
    on the common path, a plain `callback` passed straight down, the inner
    wraps never created a wrapper. Removing them changes nothing there.
  - Where an inner wrap surrounded a fresh lambda (`UsageTrackingInternalConnection`,
    `DefaultServer`, `LoadBalancedServer`, hand-written Sasl/X509 bodies), a
    throw used to be logged under `.connection`/`.authenticator` and swallowed,
    hanging the operation. NEW BEHAVIOR: it now propagates to the lambda's
    caller. A `beginAsync` chain or a hand-written `catch (Throwable)` fails the
    operation with that exception; a caller with no catch lets it unwind onto
    the calling (IO) thread. Reachable only via a driver bug; either way the
    bug is no longer masked.
    The affected set is fixed and visible in the diff: the ~10 sites where the
    wrapped expression was a lambda or helper-callback object, not a passed-through
    parameter. Each such body is an assignment or small branch followed by
    `callback.onResult(..)`; it can only throw if that downstream callback
    throws, which repeats one level up until the boundary wrap, which catches.
  - `LOGGER` fields/imports whose only user was the wrap are removed (5 classes,
    plus the `OperationHelper.LOGGER` static import in 13 operations).
- No behavior change intended beyond the callback-throw path above.
