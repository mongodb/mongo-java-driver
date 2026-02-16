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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

class VakoTest {
    private static ScheduledExecutorService executor;

    @BeforeAll
    static void beforeAll() {
        executor = Executors.newScheduledThreadPool(2);
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        executor.shutdownNow();
        com.mongodb.assertions.Assertions.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }

    @ParameterizedTest
    @CsvSource({
            "10"
    })
    void asyncCallbackLoop(final int iterations) throws Exception {
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
        System.err.printf("%n%nDONE%n%n");
    }

    private enum IterationExecutionType {
        SYNC_SAME_THREAD,
        SYNC_DIFFERENT_THREAD,
        ASYNC,
        MIXED_SYNC_SAME_AND_ASYNC
    }

    @ParameterizedTest()
    @CsvSource({
            "10, 0, SYNC_SAME_THREAD, 0, true",
//            "10, 0, SYNC_DIFFERENT_THREAD, 0, true",
            "10, 0, ASYNC, 4, true",
            "10, 4, ASYNC, 0, true",
            "1_000_000, 0, MIXED_SYNC_SAME_AND_ASYNC, 0, false",
    })
    void testThenRunDoWhileLoop(
            final int counterInitialValue,
            final int blockSyncPartOfIterationTotalSeconds,
            final IterationExecutionType executionType,
            final int delayAsyncExecutionTotalSeconds,
            final boolean verbose) throws Exception {
        System.err.printf("baselineStackDepth=%d%n%n", Thread.currentThread().getStackTrace().length);
        Duration blockSyncPartOfIterationTotalDuration = Duration.ofSeconds(blockSyncPartOfIterationTotalSeconds);
        com.mongodb.assertions.Assertions.assertTrue(
                executionType.equals(IterationExecutionType.ASYNC) || delayAsyncExecutionTotalSeconds == 0);
        Duration delayAsyncExecutionTotalDuration = Duration.ofSeconds(delayAsyncExecutionTotalSeconds);
        StartTime start = StartTime.now();
        CompletableFuture<Void> join = new CompletableFuture<>();
        asyncLoop(new Counter(counterInitialValue, verbose),
                blockSyncPartOfIterationTotalDuration, executionType, delayAsyncExecutionTotalDuration, verbose,
            (r, t) -> {
                System.err.printf("test callback completed callStackDepth=%s, r=%s, t=%s%n",
                        Thread.currentThread().getStackTrace().length, r, exceptionToString(t));
                complete(join, r, t);
        });
        System.err.printf("\tasyncLoop returned in %s%n", start.elapsed());
        join.get();
        System.err.printf("%n%nDONE%n%n");
    }

    private static void asyncLoop(
            final Counter counter,
            final Duration blockSyncPartOfIterationTotalDuration,
            final IterationExecutionType executionType,
            final Duration delayAsyncExecutionTotalDuration,
            final boolean verbose,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRunDoWhileLoop(c -> {
            sleep(blockSyncPartOfIterationTotalDuration.dividedBy(counter.initial()));
            StartTime start = StartTime.now();
            asyncPartOfIteration(counter, executionType, delayAsyncExecutionTotalDuration, verbose, c);
            if (verbose) {
                System.err.printf("\tasyncPartOfIteration returned in %s%n", start.elapsed());
            }
        }, () -> !counter.done()).finish(callback);
    }

    private static void asyncPartOfIteration(
            final Counter counter,
            final IterationExecutionType executionType,
            final Duration delayAsyncExecutionTotalDuration,
            final boolean verbose,
            final SingleResultCallback<Void> callback) {
        Runnable asyncPartOfIteration = () -> {
            counter.countDown();
            StartTime start = StartTime.now();
            callback.complete(callback);
            if (verbose) {
                System.err.printf("\tasyncPartOfIteration callback.complete returned in %s%n", start.elapsed());
            }
        };
        switch (executionType) {
            case SYNC_SAME_THREAD: {
                asyncPartOfIteration.run();
                break;
            }
            case SYNC_DIFFERENT_THREAD: {
                Thread guaranteedDifferentThread = new Thread(asyncPartOfIteration);
                guaranteedDifferentThread.start();
                join(guaranteedDifferentThread);
                break;
            }
            case ASYNC: {
                executor.schedule(asyncPartOfIteration,
                        delayAsyncExecutionTotalDuration.dividedBy(counter.initial()).toNanos(), TimeUnit.NANOSECONDS);
                break;
            }
            case MIXED_SYNC_SAME_AND_ASYNC: {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    asyncPartOfIteration.run();
                } else {
                    executor.schedule(asyncPartOfIteration,
                            delayAsyncExecutionTotalDuration.dividedBy(counter.initial()).toNanos(), TimeUnit.NANOSECONDS);
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
        private final boolean verbose;

        Counter(final int initial, final boolean verbose) {
            this.initial = initial;
            this.current = initial;
            this.verbose = verbose;
        }

        int initial() {
            return initial;
        }

        void countDown() {
            com.mongodb.assertions.Assertions.assertTrue(current > 0);
            int previous = current;
            int decremented = --current;
            if (verbose || decremented % 100_000 == 0) {
                System.err.printf("counted %d->%d callStackDepth=%d %n",
                        previous, decremented, Thread.currentThread().getStackTrace().length);
            }
        }

        boolean done() {
            if (current == 0) {
                System.err.printf("counting done callStackDepth=%d %n", Thread.currentThread().getStackTrace().length);
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
}
