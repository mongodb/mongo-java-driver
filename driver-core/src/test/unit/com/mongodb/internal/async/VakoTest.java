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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

class VakoTest {
    @ParameterizedTest
    @CsvSource({
            "10"
    })
    void asyncCallbackLoop(final int iterations) throws Exception {
        System.err.printf("baselineStackDepth=%d%n", Thread.currentThread().getStackTrace().length);
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
            if (t != null) {
                join.completeExceptionally(t);
            } else {
                join.complete(r);
            }
        });
        join.get();
        System.err.printf("%n%nDONE%n%n");
    }

    @ParameterizedTest()
    @CsvSource({
            "0, false, 0, 10",
            "0, true, 4, 10",
            "4, true, 0, 10"
    })
    void testThenRunDoWhileLoop(
            final int blockInAsyncMethodTotalSeconds,
            final boolean asyncExecution,
            final int delayAsyncExecutionTotalSeconds,
            final int counterInitialValue) throws Exception {
        Duration blockInAsyncMethodTotalDuration = Duration.ofSeconds(blockInAsyncMethodTotalSeconds);
        com.mongodb.assertions.Assertions.assertTrue(asyncExecution || delayAsyncExecutionTotalSeconds == 0);
        Duration delayAsyncExecutionTotalDuration = Duration.ofSeconds(delayAsyncExecutionTotalSeconds);
        StartTime start = StartTime.now();
        System.err.printf("baselineStackDepth=%d%n", Thread.currentThread().getStackTrace().length);
        CompletableFuture<Void> join = new CompletableFuture<>();
        asyncMethod1(blockInAsyncMethodTotalDuration, asyncExecution, delayAsyncExecutionTotalDuration, new Counter(counterInitialValue),
            (r, t) -> {
                System.err.printf("TEST callback completed callStackDepth=%s, r=%s, t=%s%n",
                        Thread.currentThread().getStackTrace().length, r, exceptionToString(t));
                if (t != null) {
                    join.completeExceptionally(t);
                } else {
                    join.complete(r);
                }
        });
        System.err.printf("asyncMethod1 returned in %s%n", start.elapsed());
        join.get();
        System.err.printf("%n%nDONE%n%n");
    }

    private static void asyncMethod1(
            final Duration blockInAsyncMethodTotalDuration,
            final boolean asyncExecution,
            final Duration delayAsyncExecutionTotalDuration,
            final Counter counter,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRunDoWhileLoop(c -> {
            sleep(blockInAsyncMethodTotalDuration.dividedBy(counter.initial()));
            StartTime start = StartTime.now();
            asyncMethod2(asyncExecution, delayAsyncExecutionTotalDuration, counter, c);
            System.err.printf("asyncMethod2 returned in %s%n", start.elapsed());
        }, () -> !counter.done()).finish(callback);
    }

    private static void asyncMethod2(
            final boolean asyncExecution,
            final Duration delayAsyncExecutionTotalDuration,
            final Counter counter,
            final SingleResultCallback<Void> callback) {
        Runnable action = () -> {
            sleep(delayAsyncExecutionTotalDuration.dividedBy(counter.initial()));
            counter.countDown();
            StartTime start = StartTime.now();
            callback.complete(callback);
            System.err.printf("asyncMethod2 callback.complete returned in %s%n", start.elapsed());
        };
        if (asyncExecution) {
            ForkJoinPool.commonPool().execute(action);
        } else {
            action.run();
        }
    }

    private static final class Counter {
        private final int initial;
        private int current;

        Counter(final int initial) {
            this.initial = initial;
            this.current = initial;
        }

        int initial() {
            return initial;
        }

        void countDown() {
            com.mongodb.assertions.Assertions.assertTrue(current > 0);
            int previous = current;
            int decremented = --current;
            System.err.printf("counted %d->%d callStackDepth=%d %n", previous, decremented, Thread.currentThread().getStackTrace().length);
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

    private static void sleep(final Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            long durationNsPart = duration.getNano();
            long durationMsPartFromNsPart = TimeUnit.MILLISECONDS.convert(duration.getNano(), TimeUnit.NANOSECONDS);
            long sleepMs = TimeUnit.MILLISECONDS.convert(duration.getSeconds(), TimeUnit.SECONDS) + durationMsPartFromNsPart;
            int sleepNs = Math.toIntExact(durationNsPart - TimeUnit.NANOSECONDS.convert(durationMsPartFromNsPart, TimeUnit.MILLISECONDS));
            System.err.printf("sleeping for %d ms %d ns%n", sleepMs, sleepNs);
            Thread.sleep(sleepMs, sleepNs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
