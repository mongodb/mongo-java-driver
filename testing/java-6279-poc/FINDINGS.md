# JAVA-6279 — class loader retention findings

Internal working notes for [JAVA-6279](https://jira.mongodb.org/browse/JAVA-6279) (*Stop BufferPoolPruner thread when last MongoClient
closes*), [GitHub issue 2029](https://github.com/mongodb/mongo-java-driver/issues/2029) and
[JAVA-5643](https://jira.mongodb.org/browse/JAVA-5643). Touches JAVA-6240 (`CommonExecutor`) in §5.

This directory began as a portable rework of Valentin Kovalenko's
[`primer` experiment](https://github.com/stIncMale/mongo-java-driver/commit/862b7d75fa0629e2b7c9cc4d6e8761b1678934dd), which hardcoded an
absolute path to one developer's `build/classes` directory and printed its results. It now resolves paths at run time, adds control
scenarios and a negative control, extends from synthetic classes to the driver itself, and compares every outcome against a stated
expectation so it can be run unattended.

Claims are tagged **[executed]** = observed by running this code, **[code]** = read from source, **[unmeasured]** = not established either
way.

Run it with `./testing/java-6279-poc/run.sh` (see [How to run](#how-to-run)). Nothing under any module's `src/main` is modified by the
harness.

## 1. Verdict summary

19 class loader scenarios and 12 executor mechanism checks, the latter under every JDK on the machine. Last full run:
all scenarios matched expectation, 50 PASS / 10 INFO / 0 FAIL. `PINNED` means the class loader was still strongly reachable after a window
of `System.gc()` nudges (10 s by default, `-Djava6279.gcWindowSeconds` to change it);
`COLLECTED` means its phantom reference was enqueued.

### The two conclusions that matter

1. **A non-terminated thread we start prevents the class loader of all driver classes — and therefore all of their static state — from being
   collected.** **[executed]**
2. **`BufferPoolPruner` is that thread, and after `MongoClient.close()` it is the only thing left holding the loader.**
   **[executed]**

The second is load-bearing and is why `driver/OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING` exists. Knowing the pruner survives `close()` is
not enough — if anything *else* also survived, fixing the pruner would not release the loader. Nothing else does, so terminating it is
sufficient.

### Driver scenarios

| Scenario                                            | Before the fix                                           | After the fix                                                                |
|-----------------------------------------------------|----------------------------------------------------------|------------------------------------------------------------------------------|
| `driver/LOAD_ONLY`                                  | COLLECTED                                                | COLLECTED — control: loading driver classes leaks nothing                    |
| `driver/TOUCH_DEFAULT_POOL`                         | **PINNED**                                               | **COLLECTED** — an empty pool now starts no thread                           |
| `driver/TOUCH_DEFAULT_POOL_THEN_DISABLE_PRUNING`    | COLLECTED                                                | COLLECTED                                                                    |
| `driver/OPEN_AND_CLOSE_CLIENT`                      | **PINNED**                                               | **COLLECTED after ~90 s** — the reported symptom, fixed                      |
| `driver/OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING` | COLLECTED                                                | COLLECTED — before the fix, this was the proof the pruner was the *only* pin |
| `…_AND_TOUCH_COMMON_EXECUTOR`                       | skipped on `main`; **PINNED** on the backpressure branch | unchanged — `CommonExecutor` is a separate pin, see §5                       |

The ~90 s is inherent, not slack: with a one minute `maxIdleTime` a released buffer is only evictable after 60 s (two prune cycles at
`maxIdleTime / 2`), and the thread then times out after the keep-alive. **Any test of this fix must allow for that tail** — hence
`-Djava6279.driverGcWindowSeconds`, default 150.

### Primer scenarios: what pins, and what does not

| Scenario                                      | Result     | Isolates                                                                                                                              |
|-----------------------------------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `primer/Inert`                                | COLLECTED  | Control: the harness can observe a child loader being collected at all.                                                               |
| `primer/StartsOwnThread`                      | **PINNED** | A thread *constructed* in a child-loaded class's static initializer pins the loader, even with a parent-loaded `Runnable`.            |
| `primer/StartsParentBuiltThread`              | COLLECTED  | Starting a `Thread` a *parent*-loaded class constructed does not pin. Isolates construction as the capture point.                     |
| `cclOnly/inherited`                           | **PINNED** | With **no child frame on the stack**, inheriting the child loader as context class loader pins it. A second, independent edge.        |
| `cclOnly/nulled`                              | COLLECTED  | Nulling that context class loader — *after* construction — closes that edge.                                                          |
| `primer/StartsOwnThreadNettyStyle`            | **PINNED** | Netty's context class loader dance does not help when the thread's own class is in the loader. Confounded by the stack frame; see §2. |
| `primer/InheritsContextClassLoader`           | **PINNED** | Confounded (stack frame present). Retained to show the stack capture dominates.                                                       |
| `primer/InheritsContextClassLoaderButNulled`  | **PINNED** | Confounded, as above.                                                                                                                 |
| `primer/InheritsContextClassLoaderNettyDance` | **PINNED** | Confounded, as above — even nulling the *calling* thread's loader before construction cannot remove a stack frame.                    |
| `primer/RegistersShutdownHook`                | **PINNED** | A shutdown hook pins even though the class starts no thread — see §7.                                                                 |
| `primer/RegistersShutdownHookNettyStyle`      | **PINNED** | Adding the context class loader nulling does not rescue it.                                                                           |
| `primer/RegistersShutdownHookParentBody`      | COLLECTED  | The only non-pinning hook shape, and it cannot call driver code.                                                                      |
| `primer/StaticSingletonExecutor`              | **PINNED** | Models `CommonExecutor`, and shows the proposed `Cleaner` fix can never run — see §5.                                                 |

### Executor mechanism checks

Run at `--release 8` under **JDK 8, 11, 17, 23 (GraalVM) and 26**, because this leans on `ScheduledThreadPoolExecutor`
*implementation* behaviour rather than documented contract, and the driver's baseline is Java 8.

| Check                                                                                | Result                                                                                    |
|--------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| The two settings alone do not reap the worker — a decision to stop is required       | `poolSize=1, queue=1` while idle at 10× the keep-alive                                    |
| One-shot scheduling self-terminates with no drain check (the `CommonExecutor` shape) | `poolSize=0` after the task runs, resurrects, back to 0                                   |
| A pending long one-shot delay survives a much shorter keep-alive                     | 3000 ms delay, 100 ms keep-alive → fired at 3002–3004 ms                                  |
| Repeated resurrection is reliable over 500 create/reap cycles                        | 0 tasks lost, 500 threads created                                                         |
| Concurrent scheduling across resurrection loses nothing (8 × 250)                    | all ran, 0 rejections, **1 thread created**                                               |
| **The conditional-reschedule design holds under contention**                         | 0 orphaned pools, 0 threads left — and **7/20 rounds orphaned with the re-check removed** |
| Reaps the worker once a periodic task is cancelled                                   | `poolSize` 1 → 0                                                                          |
| Stays reusable and resurrects on re-scheduling                                       | `isShutdown=false`, a *new* thread created                                                |
| Self-cancellation from inside the task stops the repeat, 2000 round trips            | 0 lost, 0 repeats not stopped                                                             |
| *(INFO)* The future needs safe publication                                           | 0–13 per 2000 round trips throw a swallowed `NullPointerException`                        |
| *(INFO)* Cancelled task retained in the queue without `removeOnCancelPolicy`         | `queue=1, poolSize=1` on most JDKs; JDK 8 reached 0 anyway                                |
| A generous keep-alive avoids thread churn, 25 cycles                                 | 1 ms → 25 threads; 2 s → 1 thread                                                         |

## 2. What pins the loader: two independent edges

There are **two** distinct retaining edges. Conflating them wasted time here, and they have different fixes.

### Edge A — capture at thread construction

`primer/StartsOwnThread` (PINNED) versus `primer/StartsParentBuiltThread` (COLLECTED) isolates this to the moment
`new Thread(...)` runs:

- Both run the same parent-loaded `Poc.SLEEPING_RUNNABLE`, so the executing thread holds no reference into the child loader by way of its
  task. **[code]**
- In both, the thread's context class loader is the *application* loader, not the child loader — the harness prints it. So edge B is not
  what is acting here. **[executed]**
- The only difference is which class was on the stack when the constructor ran. That alone flips the outcome. **[executed]**

`primer/StartsOwnThread` deliberately uses `new Thread(null, runnable, name, 1, false)` — no thread group, no inherited thread locals — so
the pinning cannot be attributed to inherited state. **[code]**

The exact retaining field inside `java.lang.Thread` was not identified. It was not needed, and it is a JDK implementation detail rather than
a contract. **[unmeasured]**

**Consequence:** a thread constructed from driver code pins the driver's own loader, and driver code is on that stack by definition. Nothing
can be nulled or cleared away. **The thread has to actually terminate.** This is JAVA-6279.

### Edge B — the inherited context class loader

`cclOnly/*` isolates this by constructing the thread from `Poc`, with **no child-loaded frame on the stack**, while the calling thread's
context class loader is the child loader. So edge A is absent and only edge B can act:

| Scenario                                                            | Result        |
|---------------------------------------------------------------------|---------------|
| `cclOnly/inherited`                                                 | **PINNED**    |
| `cclOnly/nulled` — context class loader nulled *after* construction | **COLLECTED** |

So the context class loader is a genuine independent edge, and nulling it closes it. **Nulling after construction is sufficient**; Netty's
dance around the calling thread is not required for this edge. **[executed]**

**Consequence:** this is the edge where a driver thread created on an application thread's behalf pins the *application's* loader.
`t.setContextClassLoader(null)` in `DaemonThreadFactory.newThread` closes it. That is a distinct bug from this ticket, and the change is
**not** in the tree — see the recommendation in §8.

### Why the `primer/Inherits*` scenarios are retained but prove nothing about edge B

Those three scenarios attempt edge B from inside a child-loaded class's `<clinit>`, which necessarily puts a child frame on the stack — so
edge A is present too and dominates. All three are PINNED, including Netty's full null-the-caller-then-restore dance. **That says nothing
about the mitigation**, and an earlier version of these notes wrongly concluded from them that nulling after construction was "too late".
They are kept only as evidence that edge A dominates whenever it is present. `cclOnly/*` is the clean isolation.

| Edge                               | Loader pinned         | Closed by                                |
|------------------------------------|-----------------------|------------------------------------------|
| A — construction capture           | the **driver's own**  | the thread terminating (JAVA-6279)       |
| B — inherited context class loader | the **application's** | nulling the CCL in `DaemonThreadFactory` |

Both are worth fixing. Neither substitutes for the other.

## 3. The driver path **[code]**

`driver-core/src/main/com/mongodb/internal/connection/PowerOfTwoBufferPool.java`:

```java
public static final PowerOfTwoBufferPool DEFAULT = new PowerOfTwoBufferPool().enablePruning();

PowerOfTwoBufferPool(final int highestPowerOfTwo, final long maxIdleTime, final TimeUnit timeUnit) {
    // ...
    pruner = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("BufferPoolPruner"));
}

PowerOfTwoBufferPool enablePruning() {
    pruner.scheduleAtFixedRate(this::prune, maxIdleTimeNanos, maxIdleTimeNanos / 2, TimeUnit.NANOSECONDS);
    return this;
}

void disablePruning() {
    pruner.shutdownNow();
}
```

Three details the scenarios confirm:

- The executor's constructor does not start a thread; `ScheduledThreadPoolExecutor` starts its core worker lazily on first submission. It is
  the `scheduleAtFixedRate` in `enablePruning()` that creates the thread. Consistent with
  `driver/LOAD_ONLY` COLLECTED versus `driver/TOUCH_DEFAULT_POOL` PINNED. **[executed]**
- `DEFAULT` is a static field with pruning enabled at class initialization, and there is no public path to
  `disablePruning()` — hence the reflective workaround in issue 2029, which
  `driver/TOUCH_DEFAULT_POOL_THEN_DISABLE_PRUNING` reproduces and shows still works. **[executed]**
- The pruner runs every `maxIdleTimeNanos / 2` **forever**, including against a permanently empty pool. See §4 on why that is worth changing
  beyond the leak itself.

The `DaemonThreadFactory` marking is why this is a memory leak rather than a JVM-shutdown hang: the thread never blocks process exit, so
nothing surfaces until an application server undeploys or redeploys, or an OSGi bundle is refreshed. Then the whole driver class graph, plus
every application class in the same loader, stays in the heap. **[code]**

`DEFAULT` is also **not owned by any `MongoClient`** — `driver-legacy`'s `DBCursor` and `DBCollection` and
`driver-benchmarks` use it with no client at all. **[code]** So the ticket's title, "stop the thread when the last
`MongoClient` closes", does not describe an implementable condition. Reference counting clients (community
[PR 2032](https://github.com/mongodb/mongo-java-driver/pull/2032), which triage deferred) would invent an ownership relation that does not
exist and still leave those consumers unaccounted for.

## 4. The fix: a self-terminating, self-resurrecting pruner — IMPLEMENTED **[executed]**

**Status: implemented on branch `JAVA-6279`** in `PowerOfTwoBufferPool`, with tests in `PowerOfTwoBufferPoolTest`. driver-core: 5820 tests,
0 failures, 0 errors; checkstyle and spotbugs clean. The harness now reports every
`driver/*` scenario COLLECTED, including `OPEN_AND_CLOSE_CLIENT`, which was the reported symptom.

Key the thread's lifetime to the thing it exists to serve — whether the pool holds anything:

- pool empty → nothing to prune → no thread should exist;
- a buffer is released → start pruning;
- `prune()` finds the pool drained → stop.

This also satisfies the requirement raised on the ticket that shutting the pruner down must not leave the pool occupying memory: under "stop
only when drained", an empty pool *is* the termination condition, so there is nothing left to clear.

### 4.1 Stop by not rescheduling, not by cancelling

Replace `scheduleAtFixedRate` with a one-shot `schedule` that **reschedules itself only if the pool still holds something**:

```java
// prune()
evictIdleBuffers();
if(

allPoolsEmpty()){
        pruningScheduled.

set(false);
    if(

allPoolsEmpty()){
        return;                                            // stop: queue empties, worker times out
        }
        if(!pruningScheduled.

compareAndSet(false,true)){
        return;                                            // a releaser owns the next run
        }
        }

schedule(this::prune, maxIdleTimeNanos /2, NANOSECONDS);

// release(buffer)
pool.

addLast(buffer);
if(pruningScheduled.

compareAndSet(false,true)){

schedule(this::prune, maxIdleTimeNanos /2, NANOSECONDS);
}
```

Three advantages over having `prune()` cancel its own periodic future:

1. **The publication hazard (§4.4) cannot arise**, because the task never needs a reference to its own
   `ScheduledFuture`.
2. **"Stop" becomes not doing something** rather than an action. No `cancel`, and `removeOnCancelPolicy` stops being load-bearing on the
   stop path.
3. **No wakeups at all when idle**, where today the pruner wakes every `maxIdleTimeNanos / 2` forever. It also drops
   `scheduleAtFixedRate`'s catch-up burst behaviour after a GC pause, which is undesirable for a pruner.

An earlier version of these notes claimed a self-rescheduling one-shot "behaves the same way" as `scheduleAtFixedRate`
because the next run is queued before the current one ends. **That was wrong**: it is true only of *unconditional*
rescheduling. Conditional rescheduling lets the queue empty, which is the whole point.

### 4.2 The executor recipe

```java
ScheduledThreadPoolExecutor pruner = new ScheduledThreadPoolExecutor(1, factory);
pruner.

setKeepAliveTime(keepAlive, unit);   // must be > 0
pruner.

allowCoreThreadTimeOut(true);        // let the core worker die when idle
pruner.

setRemoveOnCancelPolicy(true);       // recommended; see below
```

`pruner.shutdown()` when drained would be the obvious alternative and is a trap: a shut-down
`ScheduledThreadPoolExecutor` rejects further submissions, so resurrection would mean building a new executor per cycle, which means a
non-final field and a lock guarding it. Not shutting down avoids all of that.

- **`setRemoveOnCancelPolicy(true)`** — recommended, but the evidence is weaker than an earlier version of these notes claimed. The intent
  is that a cancelled task should not sit in the `DelayedWorkQueue` until its delay elapses. Most JDKs here show exactly that without it
  (`queue=1, poolSize=1`), but JDK 8 reached `queue=0, poolSize=0` anyway, so the measurement is reported as `INFO`, not asserted. Set it
  because it is free and because
  `MongoScheduledThreadPoolExecutor` already does — not because of that measurement.
- **The keep-alive is a thread churn dial, not a correctness one.** See §4.2.1 for what a short value does and does
  not cost.

#### 4.2.1 What a short keep-alive costs **[executed]**

The keep-alive is the one tuning decision in this fix, and the trade is not churn against correctness. It is churn
against **how long the class loader stays pinned**. A longer value means fewer threads and a longer leak tail. A
shorter value means prompter release and more thread creation.

What a short keep-alive does **not** cost, all measured:

| Concern | Result |
|---|---|
| Does it change the schedule? | **No.** The keep-alive applies only when the work queue is empty. While a task is pending, the worker parks on the delayed queue. Keep-alive 50 ms against a 500 ms period over 5 s: zero missed executions, one thread created, on JDK 8, 11 and 26. |
| Does it drop pending work? | **No.** A 3000 ms delay with a 100 ms keep-alive fired at 3002–3004 ms on all five JDKs. |
| Does it delay a scheduled task? | **No.** `ScheduledThreadPoolExecutor` creates the worker when it accepts the task, not when the task fires, so thread creation is paid at `schedule()` time rather than at the deadline. |
| Does it lose tasks in the die-and-resurrect race? | **No.** 500 forced create and reap cycles lost 0 tasks. 8 threads scheduling 250 tasks each lost 0 tasks and saw 0 rejections. |

What it does cost:

- **Thread churn, in one pattern only** — work that arrives at intervals *longer* than the keep-alive. Measured: a 1 ms
  keep-alive over 25 cycles created **25 threads**; a 2 s keep-alive over the same 25 cycles created **1**. The cost per
  thread is tens of microseconds, against a retry backoff of milliseconds or a prune interval of 30 seconds. The churn
  had to be forced artificially to measure it at all.
- **Observability noise**, which is the less obvious cost. `DaemonThreadFactory` names threads from a monotonic
  counter, so churn produces `CommonScheduler-1-thread-1`, `-2`, `-3` without bound. The 500-cycle check produced 500
  distinct thread names. Anything keyed on thread name — APM tools, JFR thread-start events, log correlation —
  accumulates entries for threads that no longer exist.
- **A hard floor.** The keep-alive must be greater than zero, because `allowCoreThreadTimeOut(true)` rejects zero. This
  is why `PowerOfTwoBufferPool` uses `Math.max(1, maxIdleTimeNanos / 2)`. A pool that is configured with a very small
  idle time would otherwise fail during construction.

Because the measured costs are microseconds and thread names, a short value is the better default. The pruner is safer
than the general case: prunes are `maxIdleTime / 2` apart and the queue stays non-empty during an active run, so churn
occurs only when a pool drains and then fills again after a gap.

### 4.3 The stop-versus-release race, and testing it honestly **[executed]**

The remaining risk: a releasing thread concludes "pruning is already scheduled" while `prune()` concurrently concludes
"drained, stopping". Get it wrong and buffers sit in the pool with no pruner — a class loader leak traded for a buffer leak. The check /
re-check above handles it, and Netty's `GlobalEventExecutor.TaskRunner` (§6) is the reference implementation.

Prototyped as `conditionalRescheduleDesignHoldsUnderContention`, 20 rounds × 6 releasers releasing for 400 ms, with a deliberately widened
interleaving window, on all five JDKs:

| Variant                                           | Orphaned pools     | Threads left |
|---------------------------------------------------|--------------------|--------------|
| Protocol intact                                   | **0**              | 0            |
| Re-check omitted (`-Djava6279.breakRecheck=true`) | **7 of 20 rounds** | 0            |

**The negative control is the important number, and it took three attempts to get.** The first two versions passed *even with the protocol
deliberately broken* — first because the orphan window is nanoseconds wide, then because fixed-count producers finished before the first
`prune` ran, so the stop path never overlapped a releaser. Both dead ends are recorded in the source comments.

> A concurrency test that has not been shown to fail against the broken implementation is not evidence.

That applies directly to whatever test ships with the fix.

### 4.4 The publication hazard, if you cancel a future anyway **[executed]**

Recorded because it was found the hard way, and because anyone reaching for the cancel-self design will meet it.

`scheduleAtFixedRate` **can begin running the task before it returns**, so a field assigned from its return value is not safely visible to
the task. The task reads null, dies with a `NullPointerException` — and a `ScheduledFuture`
*swallows* the throwable, so nothing is logged and nothing throws anywhere visible. The observable result is a pool holding buffers with no
pruner: a lost pruner, silently.

Rate on an otherwise idle machine: **13/2000 round trips on JDK 8, 1–2/2000 on 11 through 26, and 0/2000 on some runs of the same JDK** —
load sensitive, roughly 0.5% at worst. A 200-round-trip sample reports green about a third of the time, which is exactly what happened: a
200-round-trip version passed repeatedly, then failed once during a full run, which is the only reason it was noticed.

Reported as `INFO` and not gating the exit status: asserting that a race *does* reproduce is a flaky test by construction, and a run
observing 0 has disproved nothing.

**If you take §4.1, this cannot arise at all.** If you cancel a future anyway, publish it under the same lock that guards start and stop.

The *resurrect*-versus-die race, by contrast, could not be provoked, and `ThreadPoolExecutor.processWorkerExit` shows why: **[code]**

```java
int min = allowCoreThreadTimeOut ? 0 : corePoolSize;
if(min ==0&&!workQueue.

isEmpty())
min =1;                       // don't leave a non-empty queue unattended
```

### 4.5 What the fix does and does not achieve

Resurrection re-pins the loader, which is correct — the loader is in use again. The property that matters is that a quiet application
reaches a state with no live driver thread, bounded by roughly `maxIdleTime` to `1.5 × maxIdleTime`
after the last release (60–90 s with today's default). An undeployed application issues no further releases, so nothing resurrects.

So the loader is **not** collectable at the instant of `close()`. Any test of the fix must widen its GC window past
`maxIdleTime` — `-Djava6279.gcWindowSeconds` exists for this.

## 5. `CommonExecutor` (backpressure, JAVA-6240) **[code]** **[executed]**

Read against [stIncMale PR 3](https://github.com/stIncMale/mongo-java-driver/pull/3), branch `sleepAsync` at
`13d9cc3ec4`.

```java
public final class CommonExecutor {
    private static final CommonExecutor INSTANCE = new CommonExecutor();
    private final MongoScheduledThreadPoolExecutor singleThreadScheduler;

    private CommonExecutor() {
        singleThreadScheduler = new MongoScheduledThreadPoolExecutor(1, new DaemonThreadFactory("CommonScheduler"));
    }
}
```

`singleThreadScheduler` is **never shut down anywhere in the PR** — no `close`, no `shutdown`, no
`allowCoreThreadTimeOut`. The only `allowCoreThreadTimeOut(true)` in the diff is on
`ownedExecutorBackingClientExecutor`, which *is* shut down in `close()`. **[code]**

### Confirmed by running the harness against that branch **[executed]**

`run.sh` takes `JAVA6279_DRIVER_REPO` so it can be pointed at another checkout:

| Scenario                                     | main                     | backpressure  |
|----------------------------------------------|--------------------------|---------------|
| `OPEN_AND_CLOSE_CLIENT`                      | PINNED                   | PINNED        |
| `OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING` | COLLECTED                | **COLLECTED** |
| `…_AND_TOUCH_COMMON_EXECUTOR`                | *skipped, no such class* | **PINNED**    |

1. `CommonExecutor` is an **independent second pin**: client closed *and* pruning disabled, loader still held, the only addition being one
   `CommonExecutor.schedule` call.
2. The pin is **latent**. Creating and closing a client never touches `CommonExecutor`. Its only caller is
   `DefaultAsyncClientExecutor.scheduleCompletion` via `sleepAsync`, whose only caller is
   `RetryingAsyncCallbackSupplier` on a positive backoff — and only when the backing executor is *not* a
   `ScheduledExecutorService`. So it needs an async client, a non-scheduled executor, and a real retry backoff. The sync driver never
   reaches it, and it would not show up in a smoke test.

That narrowness is why the pruner fix remains the higher-value one, and why this is still worth doing before merge: a leak that only appears
under retry backoff gets found in production rather than in CI.

### The fix is two lines, because the scheduling is one-shot

|                               | Scheduling                                               | Does `allowCoreThreadTimeOut` suffice?                 |
|-------------------------------|----------------------------------------------------------|--------------------------------------------------------|
| `PowerOfTwoBufferPool` pruner | `scheduleAtFixedRate` — queue never empties              | **No.** Needs a decision to stop, and with it the race |
| `CommonExecutor`              | one-shot `schedule(...)` — queue empties after each task | **Yes.** Complete on its own                           |

Verified patch in `CommonExecutor-JAVA-6240.patch`, built on that branch and re-run: the scenario flips **PINNED → COLLECTED at PT30.078s**,
i.e. released exactly when the 30 s keep-alive expired. **[executed]**

```java
private static final Duration KEEP_ALIVE = Duration.ofSeconds(30);   // MUST precede INSTANCE
private static final CommonExecutor INSTANCE = new CommonExecutor();
// ...in the constructor:
singleThreadScheduler.

setKeepAliveTime(KEEP_ALIVE.toNanos(),NANOSECONDS);
        singleThreadScheduler.

allowCoreThreadTimeOut(true);
```

**Declaration order is not cosmetic.** With `KEEP_ALIVE` after `INSTANCE`, the constructor runs during `<clinit>` while
`KEEP_ALIVE` is still null: `NullPointerException: Cannot invoke "java.time.Duration.toNanos()"`. The first version of this patch had
exactly that bug and was only caught by running it. **[executed]**

It composes with the existing close path: `ScheduledCallbackCompletion.reject()` already cancels the future on
`close()`, and with `removeOnCancelPolicy` set that cancellation empties the queue, so a client closed *mid-sleep* also lets the worker
exit. `MongoScheduledThreadPoolExecutor` is used only by `CommonExecutor` and its own test, so hosting the change in `CommonExecutor` keeps
a general-purpose class untouched.

### Is repeated create/reap a problem? No — the worry inverts **[executed]**

| Check                                  | Result                                                                      |
|----------------------------------------|-----------------------------------------------------------------------------|
| 8 threads × 250 concurrent schedules   | all ran, 0 rejections, **1 thread ever created**                            |
| 500 *forced* create/reap cycles        | **0 tasks lost**, 500 threads created                                       |
| Long pending delay vs short keep-alive | 3000 ms delay, 100 ms keep-alive → fired at 3002–3004 ms, then `poolSize=0` |

Under load the thread is **reused**, because work arrives before the keep-alive expires. Churn only occurs when calls are spaced further
apart than the keep-alive — precisely when an extra thread creation is irrelevant, and every caller here is already waiting out a backoff of
milliseconds against a thread creation of tens of microseconds. The churn had to be forced artificially to measure at all.

The `~12 ms per cycle` the harness prints for the 500-cycle check is dominated by its own 10 ms `awaitPoolSize` poll granularity. **It is
not a measurement of thread creation cost.**

### The `Cleaner` proposal in the `VAKOTODO` cannot work

`CommonExecutor` carries:

> `// VAKOTODO create a ticket and leave a TODO to use Cleaner when we are at Java SE 17 to shut down internal
> executors if the class is GCed.`

`primer/StaticSingletonExecutor` models it exactly — static singleton, never-shut-down `ScheduledThreadPoolExecutor`, a
`java.lang.ref.Cleaner` registered on the singleton whose action shuts the executor down and holds no reference back. Result: **loader
PINNED, `cleaning action ran: false`.** **[executed]**

The reachability is circular, so it cannot be otherwise: the cleaning action runs only when the singleton becomes unreachable; the singleton
is a static field, so it is reachable while the class is loaded; the class is loaded while the loader lives; and the loader lives because of
the thread the action was meant to stop. A `Cleaner` frees resources when an object is *forgotten*, and a static singleton is never
forgotten. Java 17 changes nothing.

**Drop the TODO rather than filing it** — the ticket would be unimplementable as worded, and §4/§5 are the replacement.

## 6. Prior art: how Netty solves this **[code]** **[executed]**

From `netty-common` 4.2.9.Final sources, the version this repo already depends on. Netty hit this and arrived at the §4 design
independently, twice.

`GlobalEventExecutor`:

> Single-thread singleton `EventExecutor`. It starts the thread automatically and **stops it when there is no task
> pending in the task queue** for `io.netty.globalEventExecutor.quietPeriodSeconds` second (default is 1 second).

`ThreadDeathWatcher` — deprecated, same shape:

> When there is no thread to watch (i.e. all threads are dead), the daemon thread **will terminate itself, and a new
> daemon thread will be started again** when a new watch is added.

Mechanically Netty drives its own loop over its own task queue, with a periodic no-op `quietPeriodTask` as the heartbeat that wakes the
runner to re-check for emptiness, because `GlobalEventExecutor` is not backed by a
`ScheduledThreadPoolExecutor`. Same idea, hand-rolled.

### The race protocol worth copying

```java
// in the worker, on finding the queue empty:
boolean stopped = started.compareAndSet(true, false);
assert stopped;

// Check if there are pending entries added by execute() or schedule*() while we do CAS above.
if(taskQueue.

isEmpty()){
        // A) No new task was added -> safe to terminate
        // B) A new thread started and handled all the new tasks -> safe to terminate
        break;
        }

// There are pending tasks added again.
        if(!started.

compareAndSet(false,true)){
        // startThread() started a new thread and set 'started' to true
        // -> terminate this thread so the new one reads from taskQueue exclusively
        break;
        }
// New tasks were added, but this worker was faster to set 'started' to true
// -> keep this thread alive to handle them
```

Producer side is `addTask(task); if (!inEventLoop()) { startThread(); }`, with `startThread()` guarded by
`started.compareAndSet(false, true)`.

This is the check / re-check §4.1 needs, battle-tested, with all three outcomes named. Follow it rather than re-deriving it.

### Netty's class loader mitigation, and what it does cover

`GlobalEventExecutor.startThread()` also nulls the context class loader around thread creation:

```java
ClassLoader parentCCL = /* calling thread's context class loader */;

// Avoid calling classloader leaking through Thread.inheritedAccessControlContext.
setContextClassLoader(callingThread, null);
try{
final Thread t = threadFactory.newThread(taskRunner);

// See https://github.com/netty/netty/issues/7290 and https://bugs.openjdk.org/browse/JDK-7008595
setContextClassLoader(t, null);

thread =t;
    t.

start();
}finally{

setContextClassLoader(callingThread, parentCCL);
}
```

This addresses **edge B** (§2), not edge A. It stops a long-lived global thread from capturing whichever application loader happened to be
current when it started. It cannot help when the thread's own class is in the loader you want to unload — `primer/StartsOwnThreadNettyStyle`
applies the mitigation exactly and is still **PINNED**.

Per `cclOnly/*`, the null-the-caller-then-restore dance is **not required** for edge B; nulling the new thread's context class loader after
construction is sufficient.

## 7. Rejected alternatives

### The two settings alone **[executed]**

Adding `allowCoreThreadTimeOut(true)` and `setRemoveOnCancelPolicy(true)` to the existing pruner and changing nothing else does **nothing at
all**. `scheduleAtFixedRate` keeps a task in the `DelayedWorkQueue` permanently, so the worker always has something to wait for, `getTask`
never returns null and the keep-alive never expires. Measured with a period ten times the keep-alive: `poolSize=1, queue=1` while idle.
`removeOnCancelPolicy` is equally inert while nothing cancels. Somebody has to decide to stop; see §4.1.

### A JVM shutdown hook **[executed]**

Worse than useless. Four shapes measured:

| Shape                                                                                       | Result     | Can it stop the pruner? |
|---------------------------------------------------------------------------------------------|------------|-------------------------|
| Plain hook, child-loaded hook body                                                          | **PINNED** | yes                     |
| \+ context class loader nulled around creation                                              | **PINNED** | yes                     |
| \+ nulled CCL *and* parent-loaded `Runnable`, `Thread` still constructed by the child class | **PINNED** | no                      |
| `Thread` **constructed** by a parent-loaded class                                           | COLLECTED  | **no**                  |

Row 3 caught out a prediction: a parent-loaded `Runnable` and a nulled context class loader are still not enough, because edge A captures
the constructing class. Only building the `Thread` outside the loader removes the pin, and then the hook references nothing in the driver.
**The hook pins exactly to the extent that it can do its job.**

Two further problems:

- `ApplicationShutdownHooks` keeps hooks in a static map on a bootstrap-loaded class, so the hook thread, its
  `Runnable`, that class and its loader are reachable until JVM exit. Registering a hook *creates* a pin where there was none.
- It fixes the wrong problem anyway. Hooks run at JVM exit, when the process and its memory are going away. This leak bites *before* exit —
  undeploy, redeploy, OSGi refresh — while the JVM keeps running. The pruner is already a daemon thread, so it never delays exit.

### Reference counting `MongoClient`s

Community [PR 2032](https://github.com/mongodb/mongo-java-driver/pull/2032). Triage deferred it, and per §3 `DEFAULT`
has consumers with no `MongoClient` at all, so the ownership relation it needs does not exist.

## 8. Recommendation

1. ~~**`PowerOfTwoBufferPool`, on `main`, as JAVA-6279**~~ — §4. **DONE**, on branch `JAVA-6279`. One file plus its test, `driver-core`
   only.
2. **`CommonExecutor`, on the backpressure branch, as JAVA-6240** — §5. Two lines plus deleting the `Cleaner` TODO. Cheap, no race, follows
   precedent already in that PR, and stops a second pin landing. Narrower in effect than (1), but latent, so worth doing before merge.
3. **`DaemonThreadFactory` context class loader nulling** — §2 edge B. **Not applied**: tried, measured, then removed pending a decision. It
   fixes a distinct, unfiled bug — the driver pinning an *application's* loader — but it needs a scoping decision first. Code on these
   threads can no longer rely on a context class loader, `AsyncGetter` threads run application callbacks, and `ServiceLoader.load(Class)`
   reads the context class loader implicitly. Netty's version is narrower and covers only its own global executor. If it lands it wants its
   own ticket and its own commit, and it must not close this one.
4. **If JAVA-6279 stays blocked**, widening `disablePruning()`'s visibility costs nothing in public API terms (`PowerOfTwoBufferPool` is
   already `com.mongodb.internal`) and removes the need for reflection in the issue-2029 workaround. A stopgap, not a fix — the default
   still leaks.

(1) and (2) are independent: disjoint files, no conflict. Backpressure does not touch `PowerOfTwoBufferPool` at all; the only shared file is
`DaemonThreadFactory`, where that PR adds `final` and tidies javadoc.

**Whichever lands first, flip the harness's expectations as part of it**, or the suite goes red on success and gets ignored.

## 9. Loose ends not pursued

- In `driver/OPEN_AND_CLOSE_CLIENT_THEN_DISABLE_PRUNING` the thread listing taken immediately after `close()` still shows a `cluster-<id>`
  monitor thread, yet the loader is collected ~5 ms later. Either it terminated inside the GC window or it does not pin. Not investigated;
  the verdict is the same either way. **[unmeasured]**
- The exact retaining field behind edge A. A JDK implementation detail, and not needed to act. **[unmeasured]**
- An adaptive prune delay — sleep until the oldest buffer becomes evictable rather than polling on a fixed cadence — would remove the
  remaining wakeups. It needs the minimum `lastUsedNanos` across pools and changes eviction timing, so it does not belong in a bug fix.
  **[unmeasured]**
- `SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"` in the output is expected: the child loader gets the driver's compile
  classpath, which has the SLF4J API but no binding.

## How to run

```bash
./testing/java-6279-poc/run.sh            # everything
./testing/java-6279-poc/run.sh primer     # synthetic class loader scenarios, no driver code
./testing/java-6279-poc/run.sh driver     # the driver in a child class loader
./testing/java-6279-poc/run.sh executor   # mechanism checks, under every JDK found
```

No MongoDB server is needed — the driver scenarios never issue an operation, so a `MongoClient` can be created and closed against an address
nothing is listening on. `run.sh` builds `:bson:jar :driver-core:jar :driver-sync:jar`, resolves third party jars from `driver-sync`'s own
runtime classpath so versions match the build, compiles the harness into a temporary directory and runs it.

Options:

|                                               |                                                                        |
|-----------------------------------------------|------------------------------------------------------------------------|
| `JAVA_HOMES=/a:/b`                            | override JDK discovery for the `executor` mode                         |
| `JAVA6279_DRIVER_REPO=/path`                  | run the `driver` scenarios against another checkout's jars             |
| `POC_JAVA_ARGS=-Djava6279.gcWindowSeconds=45` | widen the GC window, e.g. past a fix's keep-alive                      |
| `POC_JAVA_ARGS=-Djava6279.breakRecheck=true`  | negative control for the §4.3 race protocol; that check must then FAIL |

To run the `driver` scenarios against the backpressure branch:

```bash
git worktree add --detach /tmp/bp stIncMale/sleepAsync
(cd /tmp/bp && ./gradlew -q -PskipCryptVerify=true :bson:jar :driver-core:jar :driver-sync:jar)
JAVA6279_DRIVER_REPO=/tmp/bp ./testing/java-6279-poc/run.sh driver
```

`-PskipCryptVerify=true` is needed unless `gpg` is set up for the crypt library signature check.

Exit status is 0 when every scenario matched the expectation in `Poc.java` and every gating check in
`ExecutorMechanism.java` passed.

The `driver/*` expectations now describe **fixed** behaviour, so this suite is a regression check on JAVA-6279: if
`OPEN_AND_CLOSE_CLIENT` starts reporting PINNED again, the pruner has stopped terminating. The `primer/*` expectations describe properties
of the JVM rather than of the driver, and are not expected to change.
