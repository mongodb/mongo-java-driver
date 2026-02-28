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
package com.mongodb.internal.async;

import com.mongodb.internal.async.function.AsyncCallbackLoop;
import com.mongodb.internal.async.function.LoopState;
import com.mongodb.internal.time.StartTime;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncLoopTest {
    private static final int MAX_STACK_DEPTH = 500;

    @ParameterizedTest
    @CsvSource({
            "10"
    })
    void testDemo(final int iterations) throws Exception {
        System.err.printf("baselineStackDepth=%d%n%n", Thread.currentThread().getStackTrace().length);
        CompletableFuture<Void> join = new CompletableFuture<>();
        LoopState loopState = new LoopState();
        new AsyncCallbackLoop(loopState, c -> {
            int iteration = loopState.iteration();
            System.err.printf("iteration=%d, callStackDepth=%d%n", iteration, Thread.currentThread().getStackTrace().length);
            if (!loopState.breakAndCompleteIf(() -> iteration == (iterations - 1), c)) {
                c.complete(c);
            }
        }).run((r, t) -> {
            System.err.printf("test callback completed callStackDepth=%d, r=%s, t=%s%n",
                    Thread.currentThread().getStackTrace().length, r, exceptionToString(t));
            complete(join, r, t);
        });
        join.get();
        System.err.printf("%nDONE%n%n");
    }

    private enum IterationExecutionType {
        SYNC_SAME_THREAD,
        SYNC_DIFFERENT_THREAD,
        ASYNC,
        MIXED_SYNC_SAME_THREAD_AND_ASYNC
    }

    private enum Verbocity {
        VERBOSE,
        COMPACT;

        /**
         * Every {@value}s message is printed.
         */
        private static final int COMPACTNESS = 50_000;
    }

    private enum ThreadManagement {
        NEW_THREAD_PER_TASK,
        REUSE_THREADS
    }

    @ParameterizedTest()
    @CsvSource({
            "250_000, 0, SYNC_SAME_THREAD, 0, COMPACT, 0, REUSE_THREADS",
            "250_000, 0, ASYNC, 0, COMPACT, 0, NEW_THREAD_PER_TASK",
            "250_000, 0, ASYNC, 0, COMPACT, 1, REUSE_THREADS",
            "250_000, 0, ASYNC, 0, COMPACT, 2, REUSE_THREADS",
            "250_000, 0, MIXED_SYNC_SAME_THREAD_AND_ASYNC, 0, COMPACT, 0, NEW_THREAD_PER_TASK",
            "250_000, 0, MIXED_SYNC_SAME_THREAD_AND_ASYNC, 0, COMPACT, 1, REUSE_THREADS",
            "4, 0, ASYNC, 4, VERBOSE, 1, REUSE_THREADS",
            "4, 4, ASYNC, 0, VERBOSE, 1, REUSE_THREADS",
            "250_000, 0, SYNC_DIFFERENT_THREAD, 0, COMPACT, 0, NEW_THREAD_PER_TASK",
            "250_000, 0, SYNC_DIFFERENT_THREAD, 0, COMPACT, 1, REUSE_THREADS",
    })
    void thenRunDoWhileLoopTest(
            final int counterInitialValue,
            final int blockSyncPartOfIterationTotalSeconds,
            final IterationExecutionType executionType,
            final int delayAsyncExecutionTotalSeconds,
            final Verbocity verbocity,
            final int executorSize,
            final ThreadManagement threadManagement) throws Exception {
        Duration blockSyncPartOfIterationTotalDuration = Duration.ofSeconds(blockSyncPartOfIterationTotalSeconds);
        if (executionType.equals(IterationExecutionType.SYNC_DIFFERENT_THREAD)) {
            com.mongodb.assertions.Assertions.assertTrue(
                    (executorSize > 0 && threadManagement.equals(ThreadManagement.REUSE_THREADS))
                            || (executorSize == 0 && threadManagement.equals(ThreadManagement.NEW_THREAD_PER_TASK)));
        }
        if (executionType.equals(IterationExecutionType.SYNC_SAME_THREAD)) {
            com.mongodb.assertions.Assertions.assertTrue(executorSize == 0);
            com.mongodb.assertions.Assertions.assertTrue(threadManagement.equals(ThreadManagement.REUSE_THREADS));
        }
        if (!executionType.equals(IterationExecutionType.ASYNC)) {
            com.mongodb.assertions.Assertions.assertTrue(delayAsyncExecutionTotalSeconds == 0);
        }
        if (threadManagement.equals(ThreadManagement.NEW_THREAD_PER_TASK)) {
            com.mongodb.assertions.Assertions.assertTrue(executorSize == 0);
        }
        Duration delayAsyncExecutionTotalDuration = Duration.ofSeconds(delayAsyncExecutionTotalSeconds);
        ScheduledExecutor executor = executorSize == 0 ? null : new ScheduledExecutor(executorSize, threadManagement);
        try {
            System.err.printf("baselineStackDepth=%d%n%n", Thread.currentThread().getStackTrace().length);
            StartTime start = StartTime.now();
            CompletableFuture<Void> join = new CompletableFuture<>();
            asyncLoop(new Counter(counterInitialValue, verbocity),
                    blockSyncPartOfIterationTotalDuration, executionType, delayAsyncExecutionTotalDuration, verbocity, executor,
                    (r, t) -> {
                        int stackDepth = Thread.currentThread().getStackTrace().length;
                        System.err.printf("test callback completed callStackDepth=%s, r=%s, t=%s%n",
                                stackDepth, r, exceptionToString(t));
                        assertTrue(stackDepth <= MAX_STACK_DEPTH);
                        complete(join, r, t);
                    });
            System.err.printf("\tasyncLoop method completed in %s%n", start.elapsed());
            join.get();
            System.err.printf("%nDONE%n%n");
        } finally {
            if (executor != null) {
                executor.shutdownNow();
                com.mongodb.assertions.Assertions.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
            }
        }
    }

    private static void asyncLoop(
            final Counter counter,
            final Duration blockSyncPartOfIterationTotalDuration,
            final IterationExecutionType executionType,
            final Duration delayAsyncExecutionTotalDuration,
            final Verbocity verbocity,
            @Nullable
            final ScheduledExecutor executor,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRunDoWhileLoop(c -> {
            sleep(blockSyncPartOfIterationTotalDuration.dividedBy(counter.initial()));
            StartTime start = StartTime.now();
            asyncPartOfIteration(counter, executionType, delayAsyncExecutionTotalDuration, verbocity, executor, c);
            if (verbocity.equals(Verbocity.VERBOSE)) {
                System.err.printf("\tasyncPartOfIteration method completed in %s%n", start.elapsed());
            }
        }, () -> !counter.done()).finish(callback);
    }

    private static void asyncPartOfIteration(
            final Counter counter,
            final IterationExecutionType executionType,
            final Duration delayAsyncExecutionTotalDuration,
            final Verbocity verbocity,
            @Nullable
            final ScheduledExecutor executor,
            final SingleResultCallback<Void> callback) {
        Runnable asyncPartOfIteration = () -> {
            counter.countDown();
            StartTime start = StartTime.now();
            callback.complete(callback);
            if (verbocity.equals(Verbocity.VERBOSE)) {
                System.err.printf("\tasyncPartOfIteration callback.complete method completed in %s%n", start.elapsed());
            }
        };
        switch (executionType) {
            case SYNC_SAME_THREAD: {
                asyncPartOfIteration.run();
                break;
            }
            case SYNC_DIFFERENT_THREAD: {
                if (executor == null) {
                    Thread thread = new Thread(asyncPartOfIteration);
                    thread.start();
                    join(thread);
                } else {
                    join(executor.submit(asyncPartOfIteration));
                }
                break;
            }
            case ASYNC: {
                if (executor == null) {
                    Thread thread = new Thread(() -> {
                        sleep(delayAsyncExecutionTotalDuration.dividedBy(counter.initial()));
                        asyncPartOfIteration.run();
                    });
                    thread.start();
                } else {
                    com.mongodb.assertions.Assertions.assertNotNull(executor).schedule(asyncPartOfIteration,
                            delayAsyncExecutionTotalDuration.dividedBy(counter.initial()).toNanos(), TimeUnit.NANOSECONDS);
                }
                break;
            }
            case MIXED_SYNC_SAME_THREAD_AND_ASYNC: {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    asyncPartOfIteration.run();
                } else {
                    if (executor == null) {
                        Thread thread = new Thread(() -> {
                            sleep(delayAsyncExecutionTotalDuration.dividedBy(counter.initial()));
                            asyncPartOfIteration.run();
                        });
                        thread.start();
                    } else {
                        com.mongodb.assertions.Assertions.assertNotNull(executor).schedule(asyncPartOfIteration,
                                delayAsyncExecutionTotalDuration.dividedBy(counter.initial()).toNanos(), TimeUnit.NANOSECONDS);
                    }
                }
                break;
            }
            default: {
                com.mongodb.assertions.Assertions.fail(executionType.toString());
            }
        }
    }

    private static final class Counter {
        private final int initial;
        private int current;
        private boolean doneReturnedTrue;
        private final Verbocity verbocity;

        Counter(final int initial, final Verbocity verbocity) {
            this.initial = initial;
            this.current = initial;
            this.doneReturnedTrue = false;
            this.verbocity = verbocity;
        }

        int initial() {
            return initial;
        }

        void countDown() {
            com.mongodb.assertions.Assertions.assertTrue(current > 0);
            int previous = current;
            int decremented = --current;
            if (verbocity.equals(Verbocity.VERBOSE) || decremented % Verbocity.COMPACTNESS == 0) {
                int stackDepth = Thread.currentThread().getStackTrace().length;
                assertTrue(stackDepth <= MAX_STACK_DEPTH);
                System.err.printf("counted %d->%d tid=%d callStackDepth=%d %n",
                        previous, decremented, Thread.currentThread().getId(), stackDepth);
            }
        }

        boolean done() {
            if (current == 0) {
                com.mongodb.assertions.Assertions.assertFalse(doneReturnedTrue);
                int stackDepth = Thread.currentThread().getStackTrace().length;
                assertTrue(stackDepth <= MAX_STACK_DEPTH);
                System.err.printf("counting done callStackDepth=%d %n", stackDepth);
                doneReturnedTrue = true;
                return true;
            }
            return false;
        }
    }

    private static String exceptionToString(@Nullable final Throwable t) {
        if (t == null) {
            return Objects.toString(null);
        }
        try (StringWriter sw = new StringWriter();
             PrintWriter pw = new PrintWriter(sw)) {
//            t.printStackTrace(pw);
            pw.println(t);
            pw.flush();
            return sw.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> void complete(final CompletableFuture<T> future, @Nullable final T result, @Nullable final Throwable t) {
        if (t != null) {
            future.completeExceptionally(t);
        } else {
            future.complete(result);
        }
    }

    private static void join(final Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void join(final Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sleep(final Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            long durationNsPart = duration.getNano();
            long durationMsPartFromNsPart = TimeUnit.MILLISECONDS.convert(duration.getNano(), TimeUnit.NANOSECONDS);
            long sleepMs = TimeUnit.MILLISECONDS.convert(duration.getSeconds(), TimeUnit.SECONDS) + durationMsPartFromNsPart;
            int sleepNs = Math.toIntExact(durationNsPart - TimeUnit.NANOSECONDS.convert(durationMsPartFromNsPart, TimeUnit.MILLISECONDS));
            Thread.sleep(sleepMs, sleepNs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * This {@link ScheduledThreadPoolExecutor} propagates exceptions that caused termination of a task execution,
     * causing the thread that executed the task to be terminated.
     */
    private static final class ScheduledExecutor extends ScheduledThreadPoolExecutor {
        ScheduledExecutor(final int size, final ThreadManagement threadManagement) {
            super(size, r -> {
                Thread thread = new Thread(() -> {
                    r.run();
                    if (threadManagement.equals(ThreadManagement.NEW_THREAD_PER_TASK)) {
                        terminateCurrentThread();
                    }
                });
                thread.setUncaughtExceptionHandler((t, e) -> {
                    if (e instanceof ThreadTerminationException) {
                        return;
                    }
                    t.getThreadGroup().uncaughtException(t, e);
                });
                return thread;
            });
        }

        private static void terminateCurrentThread() {
            throw ThreadTerminationException.INSTANCE;
        }

        @Override
        protected void afterExecute(final Runnable r, final Throwable t) {
            if (t instanceof ThreadTerminationException) {
                throw (ThreadTerminationException) t;
            } else if (r instanceof Future<?>) {
                Future<?> future = (Future<?>) r;
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof ThreadTerminationException) {
                            throw (ThreadTerminationException) cause;
                        }
                    } catch (Throwable e) {
                        // do nothing, we are not swallowing `e`, btw
                    }
                }
            }
        }

        private static final class ThreadTerminationException extends RuntimeException {
            static final ThreadTerminationException INSTANCE = new ThreadTerminationException();

            private ThreadTerminationException() {
                super(null, null, false, false);
            }
        }
    }
}
