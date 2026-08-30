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

package java6279;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Is a self-terminating, self-resurrecting {@code BufferPoolPruner} implementable without ever shutting the executor
 * down? This decides the shape of the JAVA-6279 fix, so it is checked rather than assumed.
 *
 * <p>The alternative -- {@code shutdown()} when drained -- is terminal: a shut-down
 * {@link ScheduledThreadPoolExecutor} rejects further submissions, so resurrection would mean building a new executor
 * each cycle, which in turn means a non-final field and a lock guarding it. The recipe checked here avoids all of
 * that:</p>
 *
 * <pre>
 *     ScheduledThreadPoolExecutor pruner = new ScheduledThreadPoolExecutor(1, factory);
 *     pruner.setKeepAliveTime(keepAlive, unit);   // must be &gt; 0
 *     pruner.allowCoreThreadTimeOut(true);        // let the core worker die when idle
 *     pruner.setRemoveOnCancelPolicy(true);       // so a cancelled periodic task leaves the queue empty
 * </pre>
 *
 * <p>{@code prune()} then cancels its own periodic future when it finds the pool drained, the worker times out and
 * exits, and a later {@code scheduleAtFixedRate} on the same executor brings a worker back.</p>
 *
 * <p>Deliberately Java 8 clean, with no dependency on the rest of this proof of concept, so that {@code run.sh} can
 * compile it at {@code --release 8} and run it under every JDK on the machine. The driver's baseline is Java 8 and
 * the behaviour being relied on is unspecified {@link ScheduledThreadPoolExecutor} implementation behaviour, not
 * contract, so "it works on the developer's JDK" is not good enough.</p>
 */
public final class ExecutorMechanism {
    /**
     * Big enough to be meaningful: the safe-publication hazard below shows up in roughly 0.5% of round trips, so a
     * sample of 200 reports green about a third of the time. Sample sizes here are chosen against measured rates.
     */
    private static final int ROUND_TRIPS = 2000;

    /**
     * Set {@code -Djava6279.breakRecheck=true} to omit the check / re-check from
     * {@link #conditionalRescheduleDesignHoldsUnderContention()}. That check must then FAIL; if it still passes, it is
     * not sensitive enough to be evidence of anything.
     */
    private static final boolean BREAK_RECHECK = Boolean.getBoolean("java6279.breakRecheck");

    private ExecutorMechanism() {
    }

    public static void main(final String... args) throws Exception {
        System.out.printf("%s %s by %s%n", System.getProperty("java.vm.name"), System.getProperty("java.version"),
                System.getProperty("java.vendor"));
        List<Check> checks = new ArrayList<Check>();
        checks.add(settingsAloneDoNotReapTheWorker());
        checks.add(oneShotSchedulingSelfTerminatesWithNoDrainCheck());
        checks.add(pendingLongDelaySurvivesAShortKeepAlive());
        checks.add(repeatedResurrectionIsReliable());
        checks.add(concurrentSchedulingAcrossResurrection());
        checks.add(conditionalRescheduleDesignHoldsUnderContention());
        checks.add(reapsWorkerWhenPeriodicTaskCancelled());
        checks.add(staysReusableAndResurrects());
        checks.add(selfCancellationFromInsideTheTaskStopsTheRepeat());
        checks.add(safePublicationOfTheFutureIsRequired());
        checks.add(cancelledTaskLingersWithoutRemoveOnCancelPolicy());
        checks.add(generousKeepAliveAvoidsThreadChurn());
        report(checks);
    }

    /** A pruner instrumented so the checks can see how many threads it ever created. */
    private static final class Pruner {
        private final ScheduledThreadPoolExecutor executor;
        private final AtomicInteger threadsCreated = new AtomicInteger();

        Pruner(final long keepAlive, final TimeUnit unit, final boolean removeOnCancel) {
            ThreadFactory factory = new ThreadFactory() {
                @Override
                public Thread newThread(final Runnable r) {
                    Thread thread = new Thread(r, "BufferPoolPruner-" + threadsCreated.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }
            };
            executor = new ScheduledThreadPoolExecutor(1, factory);
            executor.setKeepAliveTime(keepAlive, unit);
            executor.allowCoreThreadTimeOut(true);
            executor.setRemoveOnCancelPolicy(removeOnCancel);
        }

        /**
         * Schedules a periodic task that cancels itself on its first run, as a drained {@code prune()} would.
         *
         * <p>The {@code published} latch is not ceremony. {@code scheduleAtFixedRate} can start running the task
         * before it returns the future, so a task that reads a field written *after* the call can see the unwritten
         * value. See {@link #safePublicationOfTheFutureIsRequired()} — this cost an afternoon.</p>
         */
        ScheduledFuture<?> scheduleSelfCancelling(final CountDownLatch ran) {
            final AtomicReference<ScheduledFuture<?>> self = new AtomicReference<ScheduledFuture<?>>();
            final CountDownLatch published = new CountDownLatch(1);
            ScheduledFuture<?> future = executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        published.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    self.get().cancel(false);
                    ran.countDown();
                }
            }, 0, 5, TimeUnit.MILLISECONDS);
            self.set(future);
            published.countDown();
            return future;
        }

        ScheduledFuture<?> scheduleRepeating(final CountDownLatch ran) {
            return executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ran.countDown();
                }
            }, 0, 5, TimeUnit.MILLISECONDS);
        }

        void shutdownNow() {
            executor.shutdownNow();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the checks
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * The tempting one-line fix: add {@code allowCoreThreadTimeOut(true)} and {@code setRemoveOnCancelPolicy(true)} to
     * the existing pruner and change nothing else. This checks whether that is sufficient. It is not.
     *
     * <p>{@code enablePruning()} uses {@code scheduleAtFixedRate}, so the periodic task sits in the
     * {@code DelayedWorkQueue} permanently. The worker therefore always has something to wait for, {@code getTask}
     * never returns null, and the keep-alive never expires — the settings are inert. {@code removeOnCancelPolicy} is
     * likewise inert, because nothing ever cancels anything.</p>
     *
     * <p>Note this is not specific to {@code scheduleAtFixedRate}: a self-rescheduling one-shot has the same property,
     * since the next run is queued before the current one ends. The queue is only empty when pruning has genuinely
     * stopped, which is the point — <em>somebody has to decide to stop</em>. That decision, the drained check, is the
     * actual fix; these two settings are only what turns the decision into a dead thread.</p>
     */
    private static Check settingsAloneDoNotReapTheWorker() throws Exception {
        Pruner pruner = new Pruner(100, TimeUnit.MILLISECONDS, true);
        try {
            final CountDownLatch ran = new CountDownLatch(1);
            // A period long relative to the keep-alive, as the real one is: 1 minute idle time, 30 second period.
            ScheduledFuture<?> task = pruner.executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ran.countDown();
                }
            }, 0, 2, TimeUnit.SECONDS);
            if (!ran.await(5, TimeUnit.SECONDS)) {
                return Check.fail("the two settings alone do not reap the worker", "the task never ran");
            }
            // Idle for many multiples of the keep-alive, in the gap between two runs.
            Thread.sleep(1000);
            int poolSize = pruner.executor.getPoolSize();
            int queued = pruner.executor.getQueue().size();
            task.cancel(false);
            int afterCancel = awaitPoolSize(pruner, 0);
            // Passes by demonstrating that the settings are inert until something cancels.
            return Check.of("the two settings alone do not reap the worker -- a decision to stop is required",
                    poolSize == 1 && queued == 1 && afterCancel == 0,
                    "idle 10x the keep-alive with the periodic task still scheduled: poolSize=" + poolSize
                            + ", queue=" + queued + " (thread alive, loader still pinned); "
                            + "poolSize=" + afterCancel + " only once the task stops being scheduled");
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * The distinction that decides how much work each fix is: <em>one-shot</em> scheduling needs no drain check at all.
     *
     * <p>{@code PowerOfTwoBufferPool.enablePruning()} uses {@code scheduleAtFixedRate}, so its queue is never empty and
     * {@code allowCoreThreadTimeOut} can never fire — see {@link #settingsAloneDoNotReapTheWorker()}. But
     * {@code CommonExecutor.schedule} uses one-shot {@code schedule(...)}, so once the scheduled task has run the queue
     * really is empty, the worker times out on its own, and nothing has to decide to stop. For that shape the two
     * settings ARE the whole fix, with none of the stop-versus-release race.</p>
     */
    private static Check oneShotSchedulingSelfTerminatesWithNoDrainCheck() throws Exception {
        Pruner pruner = new Pruner(100, TimeUnit.MILLISECONDS, true);
        try {
            final CountDownLatch ran = new CountDownLatch(1);
            pruner.executor.schedule(new Runnable() {
                @Override
                public void run() {
                    ran.countDown();
                }
            }, 10, TimeUnit.MILLISECONDS);
            if (!ran.await(5, TimeUnit.SECONDS)) {
                return Check.fail("one-shot scheduling self-terminates with no drain check", "the task never ran");
            }
            int afterRun = awaitPoolSize(pruner, 0);
            // And it must still resurrect for the next scheduled task.
            final CountDownLatch ranAgain = new CountDownLatch(1);
            pruner.executor.schedule(new Runnable() {
                @Override
                public void run() {
                    ranAgain.countDown();
                }
            }, 10, TimeUnit.MILLISECONDS);
            boolean resurrected = ranAgain.await(5, TimeUnit.SECONDS);
            int afterSecond = awaitPoolSize(pruner, 0);
            return Check.of("one-shot scheduling self-terminates with no drain check (the CommonExecutor shape)",
                    afterRun == 0 && resurrected && afterSecond == 0 && pruner.threadsCreated.get() > 1,
                    "poolSize=" + afterRun + " after the one-shot ran, task ran again=" + resurrected
                            + ", poolSize=" + afterSecond + " after that, threads ever created="
                            + pruner.threadsCreated.get());
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * The safety question for the {@code CommonExecutor} fix: with {@code allowCoreThreadTimeOut(true)} and a keep-alive
     * much SHORTER than a pending one-shot delay, is that pending task still honoured, or can the worker time out and
     * drop it?
     *
     * <p>This matters because {@code sleepAsync} delays are arbitrary — a retry backoff may be seconds while a sensible
     * keep-alive is shorter. Losing a pending task would hang the callback, which is far worse than a leaked thread.</p>
     *
     * <p>Safe by construction, per {@code ThreadPoolExecutor.processWorkerExit}: if the last worker exits while the
     * queue is non-empty, a replacement is added. Checked anyway.</p>
     */
    private static Check pendingLongDelaySurvivesAShortKeepAlive() throws Exception {
        Pruner pruner = new Pruner(100, TimeUnit.MILLISECONDS, true);
        try {
            long delayMillis = 3000;
            final CountDownLatch ran = new CountDownLatch(1);
            long scheduledAt = System.nanoTime();
            pruner.executor.schedule(new Runnable() {
                @Override
                public void run() {
                    ran.countDown();
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
            boolean honoured = ran.await(delayMillis * 3, TimeUnit.MILLISECONDS);
            long actualMillis = (System.nanoTime() - scheduledAt) / 1_000_000L;
            int afterRun = awaitPoolSize(pruner, 0);
            // Late is as bad as lost for a callback, so require it within a generous window of the requested delay.
            boolean onTime = honoured && actualMillis < delayMillis * 2;
            return Check.of("a pending long one-shot delay survives a much shorter keep-alive",
                    onTime && afterRun == 0,
                    "keep-alive=100ms, delay=" + delayMillis + "ms, ran=" + honoured + " after " + actualMillis
                            + "ms, poolSize=" + afterRun + " once it had run");
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * {@code CommonExecutor} is a singleton shared by every {@code MongoClient}, so with a short keep-alive its worker
     * may be created and reaped over and over. Two questions: is that <em>reliable</em>, and what does it cost?
     *
     * <p>Each iteration schedules a one-shot, waits for it, then waits for the pool to drain to zero — so every
     * iteration crosses the die/resurrect boundary deliberately, which is the worst case rather than the typical one.</p>
     */
    private static Check repeatedResurrectionIsReliable() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            int iterations = 500;
            int notRun = 0;
            long startNanos = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                final CountDownLatch ran = new CountDownLatch(1);
                pruner.executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        ran.countDown();
                    }
                }, 0, TimeUnit.MILLISECONDS);
                if (!ran.await(5, TimeUnit.SECONDS)) {
                    notRun++;
                }
                awaitPoolSize(pruner, 0);
            }
            long elapsedMicrosPerCycle = (System.nanoTime() - startNanos) / 1000L / iterations;
            int threads = pruner.threadsCreated.get();
            return Check.of("repeated resurrection is reliable over " + iterations + " create/reap cycles",
                    notRun == 0 && threads > iterations / 2,
                    "tasks never run=" + notRun + ", threads ever created=" + threads
                            + " (churn really happened), ~" + elapsedMicrosPerCycle + "us per full cycle");
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * The multi-client case: several threads scheduling concurrently while the worker is dying. If resurrection lost a
     * task here, a {@code sleepAsync} callback would never complete — a hang, not a leak.
     */
    private static Check concurrentSchedulingAcrossResurrection() throws Exception {
        final Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            final int producers = 8;
            final int perProducer = 250;
            final CountDownLatch allRan = new CountDownLatch(producers * perProducer);
            final CountDownLatch go = new CountDownLatch(1);
            final AtomicInteger rejected = new AtomicInteger();
            Thread[] threads = new Thread[producers];
            for (int p = 0; p < producers; p++) {
                threads[p] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            go.await();
                            for (int i = 0; i < perProducer; i++) {
                                try {
                                    pruner.executor.schedule(new Runnable() {
                                        @Override
                                        public void run() {
                                            allRan.countDown();
                                        }
                                    }, 0, TimeUnit.MILLISECONDS);
                                } catch (java.util.concurrent.RejectedExecutionException e) {
                                    rejected.incrementAndGet();
                                    allRan.countDown();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, "producer-" + p);
                threads[p].start();
            }
            go.countDown();
            boolean everythingRan = allRan.await(30, TimeUnit.SECONDS);
            for (Thread t : threads) {
                t.join(5000);
            }
            int afterwards = awaitPoolSize(pruner, 0);
            return Check.of("concurrent scheduling across resurrection loses nothing ("
                            + producers + " threads x " + perProducer + ")",
                    everythingRan && rejected.get() == 0 && afterwards == 0,
                    "all tasks ran=" + everythingRan + ", rejections=" + rejected.get()
                            + ", outstanding=" + allRan.getCount() + ", poolSize afterwards=" + afterwards
                            + ", threads ever created=" + pruner.threadsCreated.get());
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * Prototypes the design this points to for {@code PowerOfTwoBufferPool}: replace {@code scheduleAtFixedRate} with a
     * one-shot {@code schedule} that <em>conditionally reschedules itself</em> — next run only if the pool still holds
     * something. Draining then makes the queue empty all by itself, so the worker times out; a later release schedules
     * again.
     *
     * <p>Two advantages over cancelling one's own periodic future:</p>
     * <ul>
     *     <li>The task never needs a reference to its own {@code ScheduledFuture}, so the safe-publication hazard in
     *     {@link #safePublicationOfTheFutureIsRequired()} cannot arise at all.</li>
     *     <li>"Stop" becomes <em>not doing something</em> rather than an action, which is easier to reason about.</li>
     * </ul>
     *
     * <p>The stop-versus-release race remains and still needs check / re-check. This models it with Netty's
     * {@code GlobalEventExecutor} protocol and asserts the invariant that actually matters: once everything is
     * quiescent, the pool must be empty (nothing was orphaned) and no thread may remain.</p>
     */
    private static Check conditionalRescheduleDesignHoldsUnderContention() throws Exception {
        final Pruner pruner = new Pruner(50, TimeUnit.MILLISECONDS, true);
        try {
            int rounds = 20;
            int orphanedRounds = 0;
            int threadLeftRounds = 0;
            for (int round = 0; round < rounds; round++) {
                final java.util.concurrent.ConcurrentLinkedDeque<Object> pool =
                        new java.util.concurrent.ConcurrentLinkedDeque<Object>();
                final java.util.concurrent.atomic.AtomicBoolean pruningScheduled =
                        new java.util.concurrent.atomic.AtomicBoolean();
                final AtomicInteger pruneRuns = new AtomicInteger();
                final Runnable[] prune = new Runnable[1];
                prune[0] = new Runnable() {
                    @Override
                    public void run() {
                        pruneRuns.incrementAndGet();
                        pool.clear();                                   // stands in for evicting idle buffers
                        if (pool.isEmpty()) {
                            // Widen the interleaving window deliberately. The orphan case needs a releaser to add an
                            // item AND fail its CAS in the gap between this emptiness check and the store below -- a
                            // window of nanoseconds in real code, which no realistic number of iterations would hit.
                            // Without this the negative control below passes and the whole check proves nothing.
                            for (int spin = 0; spin < 2000; spin++) {
                                Thread.yield();
                            }
                            // Mark ourselves stopped, then RE-CHECK, exactly as Netty's TaskRunner does.
                            pruningScheduled.set(false);
                            if (BREAK_RECHECK) {
                                // Negative control: the naive "empty, so stop" with no re-check. Proves this check has
                                // teeth -- if omitting the protocol still passed, a PASS above would mean nothing.
                                return;
                            }
                            if (pool.isEmpty()) {
                                return;                                 // safe to stop: nothing left
                            }
                            if (!pruningScheduled.compareAndSet(false, true)) {
                                return;                                 // a releaser scheduled a run; it owns it now
                            }
                        }
                        pruner.executor.schedule(prune[0], 1, TimeUnit.MILLISECONDS);
                    }
                };

                // Releasers must keep going long enough to overlap the pruner's STOP path, otherwise the interleaving
                // never occurs and the negative control passes. A fixed iteration count finishes in microseconds while
                // the first prune is still 1ms away, which is exactly the mistake this replaced.
                final int producers = 6;
                final long releaseForNanos = TimeUnit.MILLISECONDS.toNanos(400);
                final CountDownLatch go = new CountDownLatch(1);
                Thread[] threads = new Thread[producers];
                for (int p = 0; p < producers; p++) {
                    threads[p] = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                go.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            long until = System.nanoTime() + releaseForNanos;
                            while (System.nanoTime() < until) {
                                pool.addLast(new Object());             // "release(buffer)"
                                if (pruningScheduled.compareAndSet(false, true)) {
                                    pruner.executor.schedule(prune[0], 1, TimeUnit.MILLISECONDS);
                                }
                                Thread.yield();
                            }
                        }
                    }, "releaser-" + p);
                    threads[p].start();
                }
                go.countDown();
                for (Thread t : threads) {
                    t.join(10_000);
                }
                // Let things settle: any pruner still scheduled must get a chance to drain the pool.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (System.nanoTime() < deadline && !(pool.isEmpty() && pruner.executor.getPoolSize() == 0)) {
                    Thread.sleep(10);
                }
                if (!pool.isEmpty()) {
                    orphanedRounds++;   // buffers left with no pruner coming -- the failure that matters
                }
                if (pruner.executor.getPoolSize() != 0) {
                    threadLeftRounds++; // thread never died -- the class loader would stay pinned
                }
            }
            return Check.of("the conditional-reschedule design holds under contention (" + rounds
                            + " rounds x 6 releasers releasing for 400ms)",
                    orphanedRounds == 0 && threadLeftRounds == 0,
                    "rounds leaving an orphaned non-empty pool=" + orphanedRounds
                            + ", rounds leaving a live thread=" + threadLeftRounds);
        } finally {
            pruner.shutdownNow();
        }
    }

    /** The load-bearing one: does the core worker actually die, leaving nothing to pin the class loader? */
    private static Check reapsWorkerWhenPeriodicTaskCancelled() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            CountDownLatch ran = new CountDownLatch(1);
            ScheduledFuture<?> task = pruner.scheduleRepeating(ran);
            if (!ran.await(5, TimeUnit.SECONDS)) {
                return Check.fail("reaps the worker when the periodic task is cancelled", "the task never ran");
            }
            int poolSizeWhileScheduled = pruner.executor.getPoolSize();
            task.cancel(false);
            int poolSize = awaitPoolSize(pruner, 0);
            return Check.of("reaps the worker when the periodic task is cancelled",
                    poolSizeWhileScheduled == 1 && poolSize == 0,
                    "poolSize " + poolSizeWhileScheduled + " while scheduled, " + poolSize + " after cancel");
        } finally {
            pruner.shutdownNow();
        }
    }

    /** Resurrection has to work on the same executor, or the fix needs a mutable field and a lock. */
    private static Check staysReusableAndResurrects() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            CountDownLatch firstRan = new CountDownLatch(1);
            ScheduledFuture<?> first = pruner.scheduleRepeating(firstRan);
            boolean firstOk = firstRan.await(5, TimeUnit.SECONDS);
            first.cancel(false);
            awaitPoolSize(pruner, 0);
            boolean neverShutDown = !pruner.executor.isShutdown();

            CountDownLatch secondRan = new CountDownLatch(1);
            ScheduledFuture<?> second = pruner.scheduleRepeating(secondRan);
            boolean secondOk = secondRan.await(5, TimeUnit.SECONDS);
            second.cancel(false);
            int threads = pruner.threadsCreated.get();
            return Check.of("stays reusable and resurrects a worker on re-scheduling",
                    firstOk && secondOk && neverShutDown && threads > 1,
                    "isShutdown=" + pruner.executor.isShutdown() + ", threads ever created=" + threads
                            + " (>1 proves the first worker died rather than lingering)");
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * {@code prune()} has no handle on its own future unless one is stashed for it, and cancelling a periodic task
     * from inside its own run is exactly the case the JDK documents least clearly. Round-tripped to probe the
     * resurrect-versus-die race in {@code ThreadPoolExecutor.processWorkerExit}.
     */
    private static Check selfCancellationFromInsideTheTaskStopsTheRepeat() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            int neverRan = 0;
            int repeatNotStopped = 0;
            for (int i = 0; i < ROUND_TRIPS; i++) {
                CountDownLatch ran = new CountDownLatch(1);
                ScheduledFuture<?> task = pruner.scheduleSelfCancelling(ran);
                if (!ran.await(5, TimeUnit.SECONDS)) {
                    neverRan++;     // a lost pruner: buffers would sit in the pool with nobody to prune them
                    continue;
                }
                Thread.sleep(5);    // give the repeat a chance to misfire
                if (!task.isCancelled()) {
                    repeatNotStopped++;
                }
            }
            int threads = pruner.threadsCreated.get();
            return Check.of("self-cancellation from inside the task stops the repeat, " + ROUND_TRIPS + " round trips",
                    neverRan == 0 && repeatNotStopped == 0 && threads > 1,
                    "lost pruners=" + neverRan + ", repeats not stopped=" + repeatNotStopped
                            + ", threads ever created=" + threads);
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * A hazard the fix itself has to handle, found the hard way while writing these checks.
     *
     * <p>If {@code prune()} is to cancel its own periodic future, it needs a reference to that future — but
     * {@code scheduleAtFixedRate} can begin running the task before it returns, so a field assigned from its return
     * value is not safely visible to the task. The task then reads null, dies with a {@link NullPointerException},
     * and the executor cancels the repeat. Worse, a {@code ScheduledFuture} swallows the throwable: nothing is logged
     * and nothing throws where it would be noticed. The observable result is a pool with buffers in it and no pruner
     * — a lost pruner, which is the exact failure mode the fix must not have.</p>
     *
     * <p>Measured at roughly 0.5% of round trips on an otherwise idle machine, which is low enough that a small test
     * sample will happily report green.</p>
     *
     * <p>So the fix must publish the future under whatever lock already guards start/stop, not simply assign it after
     * scheduling.</p>
     */
    private static Check safePublicationOfTheFutureIsRequired() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, true);
        try {
            int taskThrew = 0;
            for (int i = 0; i < ROUND_TRIPS; i++) {
                final CountDownLatch ran = new CountDownLatch(1);
                final AtomicReference<ScheduledFuture<?>> self = new AtomicReference<ScheduledFuture<?>>();
                final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
                // Deliberately UNSAFE: no publication barrier, exactly the naive implementation.
                ScheduledFuture<?> task = pruner.executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            self.get().cancel(false);
                        } catch (Throwable t) {
                            thrown.set(t);
                        } finally {
                            ran.countDown();
                        }
                    }
                }, 0, 5, TimeUnit.MILLISECONDS);
                self.set(task);
                ran.await(2, TimeUnit.SECONDS);
                if (thrown.get() != null) {
                    taskThrew++;
                }
                task.cancel(false);
            }
            // Informational, never a failure. Asserting that a race *does* reproduce is a flaky test by construction:
            // the rate varies by JDK and by machine load, and a run that happens to observe 0 has not disproved
            // anything. The requirement stands on the reasoning plus the runs that did observe it; this number is here
            // to show the hazard is not hypothetical and to give a sense of how easily a small sample misses it.
            return Check.informational(
                    "the future needs safe publication, or prune() reads null and the repeat dies silently",
                    taskThrew + "/" + ROUND_TRIPS + " round trips threw NullPointerException inside the task, "
                            + "swallowed by the ScheduledFuture"
                            + (taskThrew == 0 ? " (not observed on this run -- the hazard is still real, see FINDINGS)"
                                              : ""));
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * Why {@code setRemoveOnCancelPolicy(true)} is part of the recipe and not a nicety: without it a cancelled
     * periodic task stays in the {@code DelayedWorkQueue} until its delay elapses, so the queue is not empty, so the
     * worker has something to wait on and does not time out. With a realistic one minute period that is a thread
     * loitering for up to a minute past the drain -- which is most of what the ticket is trying to avoid.
     */
    private static Check cancelledTaskLingersWithoutRemoveOnCancelPolicy() throws Exception {
        Pruner pruner = new Pruner(1, TimeUnit.MILLISECONDS, false);
        try {
            CountDownLatch ran = new CountDownLatch(1);
            // A long period, so that "still queued" and "already elapsed" are distinguishable.
            ScheduledFuture<?> task = pruner.executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ran.countDown();
                }
            }, 0, 10, TimeUnit.SECONDS);
            if (!ran.await(5, TimeUnit.SECONDS)) {
                return Check.fail("cancelled task lingers in the queue without removeOnCancelPolicy",
                        "the task never ran");
            }
            task.cancel(false);
            Thread.sleep(300);
            int queued = pruner.executor.getQueue().size();
            int poolSize = pruner.executor.getPoolSize();
            // Informational, for the same reason as safePublicationOfTheFutureIsRequired: this observes
            // implementation behaviour that varies by JDK. JDK 8 has been seen to reach queue=0, poolSize=0 here
            // anyway. The recommendation to set the policy does not rest on this measurement -- it rests on the
            // deterministic checks above, all of which pass WITH the policy set on every JDK, and on the policy being
            // free. MongoScheduledThreadPoolExecutor already sets it.
            return Check.informational("cancelled task retained in the queue without removeOnCancelPolicy",
                    "queue=" + queued + ", poolSize=" + poolSize
                            + (queued > 0 || poolSize > 0
                                    ? " -- the worker is still there, hence setRemoveOnCancelPolicy(true)"
                                    : " -- this JDK dropped it anyway; the policy is still recommended, see FINDINGS"));
        } finally {
            pruner.shutdownNow();
        }
    }

    /**
     * The keep-alive is a thread churn dial, not a correctness one. A 1 ms keep-alive creates one thread per
     * schedule/cancel cycle; a generous one lets a busy application reuse a single worker while a quiet one still
     * eventually drops to zero. Set it to something on the order of {@code maxIdleTime}.
     */
    private static Check generousKeepAliveAvoidsThreadChurn() throws Exception {
        Pruner churny = new Pruner(1, TimeUnit.MILLISECONDS, true);
        Pruner calm = new Pruner(2, TimeUnit.SECONDS, true);
        try {
            int cycles = 25;
            for (int i = 0; i < cycles; i++) {
                roundTrip(churny);
                roundTrip(calm);
            }
            int churnyThreads = churny.threadsCreated.get();
            int calmThreads = calm.threadsCreated.get();
            return Check.of("a generous keep-alive avoids thread churn over " + cycles + " cycles",
                    calmThreads < churnyThreads,
                    "1ms keep-alive created " + churnyThreads + " threads, 2s keep-alive created " + calmThreads);
        } finally {
            churny.shutdownNow();
            calm.shutdownNow();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------------

    private static void roundTrip(final Pruner pruner) throws Exception {
        CountDownLatch ran = new CountDownLatch(1);
        ScheduledFuture<?> task = pruner.scheduleSelfCancelling(ran);
        ran.await(5, TimeUnit.SECONDS);
        task.cancel(false);
        Thread.sleep(5);
    }

    private static int awaitPoolSize(final Pruner pruner, final int target) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (pruner.executor.getPoolSize() == target) {
                return target;
            }
            Thread.sleep(10);
        }
        return pruner.executor.getPoolSize();
    }

    private static final class Check {
        private final String name;
        private final boolean passed;
        private final boolean informational;
        private final String detail;

        private Check(final String name, final boolean passed, final boolean informational, final String detail) {
            this.name = name;
            this.passed = passed;
            this.informational = informational;
            this.detail = detail;
        }

        static Check of(final String name, final boolean passed, final String detail) {
            return new Check(name, passed, false, detail);
        }

        static Check fail(final String name, final String detail) {
            return new Check(name, false, false, detail);
        }

        /** Reports a measurement without gating the exit status. For hazards whose reproduction rate is stochastic. */
        static Check informational(final String name, final String detail) {
            return new Check(name, true, true, detail);
        }

        String label() {
            return informational ? "INFO" : passed ? "PASS" : "FAIL";
        }
    }

    private static void report(final List<Check> checks) {
        boolean allPassed = true;
        System.out.println();
        for (Check check : checks) {
            allPassed &= check.passed;
            System.out.printf("  %-4s %s%n", check.label(), check.name);
            System.out.printf("       %s%n", check.detail);
        }
        System.out.println();
        System.out.println(allPassed
                ? "the self-terminating, self-resurrecting pruner is implementable on this JDK"
                : "AT LEAST ONE CHECK FAILED ON THIS JDK -- the mechanism cannot be relied on");
        if (!allPassed) {
            System.exit(1);
        }
    }
}
