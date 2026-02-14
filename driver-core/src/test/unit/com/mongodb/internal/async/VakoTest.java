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
import com.mongodb.lang.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

class VakoTest {
    @ParameterizedTest
    @CsvSource({"false, 20", "true, 20"})
    void asyncCallbackLoop(final boolean optimized, final int iterations) {
        System.err.printf("baselineStackDepth=%d%n", Thread.currentThread().getStackTrace().length);
        LoopState loopState = new LoopState();
        new AsyncCallbackLoop(optimized, loopState, c -> {
            int iteration = loopState.iteration();
            System.err.printf("iteration=%d, callStackDepth=%d%n", iteration, Thread.currentThread().getStackTrace().length);
            if (!loopState.breakAndCompleteIf(() -> iteration == (iterations - 1), c)) {
                c.complete(c);
            }
        }).run((r, t) -> {
            System.err.printf("test callback completed callStackDepth=%d, r=%s, t=%s%n",
                    Thread.currentThread().getStackTrace().length, r, exceptionToString(t));
        });
    }

    @ParameterizedTest
    @CsvSource({"false, 20", "true, 20"})
    void testA(final boolean optimized, final int counterValue) {
        System.err.printf("baselineStackDepth=%d%n", Thread.currentThread().getStackTrace().length);
        asyncMethod1A(optimized, new Counter(counterValue), (r, t) -> {
            System.err.printf("test callback completed callStackDepth=%s, r=%s, t=%s%n",
                    Thread.currentThread().getStackTrace().length, r, exceptionToString(t));
        });
    }

    private static void asyncMethod1A(final boolean optimized, final Counter counter, final SingleResultCallback<Void> callback) {
        beginAsync().thenRunDoWhileLoop(optimized, c -> {
            asyncMethod2A(counter, c);
        }, () -> !counter.done()).finish(callback);
    }

    private static void asyncMethod2A(final Counter counter, final SingleResultCallback<Void> callback) {
        counter.countDown();
        callback.complete(callback);
    }

    @ParameterizedTest
    @ValueSource(ints = {10})
    void testB(final int counterValue) {
        AtomicInteger stackDepthUnoptimized = new AtomicInteger();
        AtomicInteger stackDepthOptimized = new AtomicInteger();
        asyncMethod1B(false, new Counter(counterValue), (r, t) -> {
            stackDepthUnoptimized.set(Thread.currentThread().getStackTrace().length);
            System.err.printf("test callback completed callStackDepth=%s, r=%s, t=%s%n",
                    stackDepthUnoptimized, r, exceptionToString(t));
        });
        asyncMethod1B(true, new Counter(counterValue), (r, t) -> {
            stackDepthOptimized.set(Thread.currentThread().getStackTrace().length);
            System.err.printf("test callback completed callStackDepth=%s, r=%s, t=%s%n",
                    stackDepthOptimized, r, exceptionToString(t));
        });
        System.err.printf("test completed baselineStackDepth=%d, stackDepthUnoptimized=%s, stackDepthOptimized=%s%n",
                Thread.currentThread().getStackTrace().length, stackDepthOptimized, stackDepthUnoptimized);
    }

    private static void asyncMethod1B(final boolean optimized, final Counter counter, final SingleResultCallback<Void> callback) {
        asyncMethod2B(counter, new Callback(optimized, counter, callback));
    }

    private static void asyncMethod2B(final Counter counter, final SingleResultCallback<Void> callback) {
        counter.countDown();
        callback.complete(callback);
    }

    private static final class Callback implements SingleResultCallback<Void> {
        private final boolean optimized;
        private Counter counter;
        private SingleResultCallback<Void> callback;

        Callback(final boolean optimized, final Counter counter, final SingleResultCallback<Void> callback) {
            this.optimized = optimized;
            this.counter = counter;
            this.callback = callback;
        }

        Counter takeCounter() {
            Counter localCounter = com.mongodb.assertions.Assertions.assertNotNull(counter);
            counter = null;
            return localCounter;
        }

        void setCounter(final Counter counter) {
            com.mongodb.assertions.Assertions.assertNull(this.counter);
            this.counter = counter;
        }

        SingleResultCallback<Void> takeCallback() {
            SingleResultCallback<Void> localCallback = com.mongodb.assertions.Assertions.assertNotNull(callback);
            callback = null;
            return localCallback;
        }

        void setCallback(final SingleResultCallback<Void> callback) {
            com.mongodb.assertions.Assertions.assertNull(this.callback);
            this.callback = callback;
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            SingleResultCallback<Void> localCallback = takeCallback();
            beginAsync().thenRun((c) -> {
                System.err.printf("thenRun%n");
                Counter localCounter = takeCounter();
                if (t != null) {
                    System.err.printf("exception t=%s%n", exceptionToString(t));
                    c.completeExceptionally(t);
                } else if (localCounter.done()) {
                    c.complete(c);
                } else {
                    asyncMethod2B(localCounter, new Callback(optimized, localCounter, localCallback));
                }
            }).finish((r, t2) -> {
                System.err.printf("finish r=%s, t=%s%n", r, exceptionToString(t2));
                localCallback.onResult(r, t);
            });
        }
    }

    private static final class Counter {
        private int v;

        Counter(final int v) {
            this.v = v;
        }

        void countDown() {
            com.mongodb.assertions.Assertions.assertTrue(v > 0);
            v--;
            System.err.printf("counted %d->%d callStackDepth=%d %n", v + 1, v, Thread.currentThread().getStackTrace().length);
        }

        boolean done() {
            if (v == 0) {
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
            t.printStackTrace(pw);
            pw.flush();
            return sw.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

/*

        c3.complete{
            //c3...
            c2.complete{
                //c2...
                c1.complete{
                    //c1...
                }
        }

        c3.complete{
            //c3...
            chain.add(c2)
        }
        chain.run() -> c2.complete{
            //c2...
            chain.add(c1)
        }
        chain.run() -> c1.complete{
            //c1...
        }

         */
