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
package com.mongodb.internal.thread;

import com.mongodb.internal.time.StartTime;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultAsyncClientExecutorTest {
    private static final long SLEEP_DURATION_MILLIS = 200;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeEach
    void beforeEach() {
        executorService = Executors.newSingleThreadExecutor();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void afterEach() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, SLEEP_DURATION_MILLIS})
    void sleepAsync(final long durationMs) {
        Duration duration = Duration.ofMillis(durationMs);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertSleepAsync(backedByExecutorService, duration),
                    () -> assertSleepAsync(backedByScheduledExecutorService, duration)
            );
        }
    }

    private static void assertSleepAsync(
            final DefaultAsyncClientExecutor clientExecutor, final Duration duration) throws Exception {
        StartTime startTime = StartTime.now();
        CompletableFuture<Duration> callbackDelayFuture = new CompletableFuture<>();
        CompletableFuture<Thread> callbackThreadFuture = new CompletableFuture<>();
        clientExecutor.sleepAsync(duration, (result, t) -> {
            if (t != null) {
                callbackDelayFuture.completeExceptionally(t);
            } else {
                callbackDelayFuture.complete(startTime.elapsed());
            }
            callbackThreadFuture.complete(Thread.currentThread());
        });
        // if `duration` is zero, the futures are expected to be completed synchronously
        long timeoutMs = duration.toMillis() * 2;
        Duration actualCallbackDelay = callbackDelayFuture.get(timeoutMs, MILLISECONDS);
        Thread actualCallbackThread = callbackThreadFuture.get(timeoutMs, MILLISECONDS);
        assertTrue(actualCallbackDelay.compareTo(duration) >= 0);
        if (duration.isZero()) {
            assertSame(Thread.currentThread(), actualCallbackThread);
        } else {
            assertTrue(actualCallbackDelay.compareTo(duration.multipliedBy(2)) < 0);
            assertNotSame(Thread.currentThread(), actualCallbackThread);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, SLEEP_DURATION_MILLIS})
    void closeBeforeSleepAsync(final long durationMs) {
        Duration duration = Duration.ofMillis(durationMs);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseOrBackingExecutorShutdownBeforeSleepAsync(backedByExecutorService, duration, backedByExecutorService::close),
                    () -> assertCloseOrBackingExecutorShutdownBeforeSleepAsync(backedByScheduledExecutorService, duration, backedByScheduledExecutorService::close)
            );
        }
    }

    /**
     * {@link AsyncClientExecutor#close()} and {@link com.mongodb.connection.NettyTransportSettings.Builder#eventLoopGroup(EventLoopGroup)}
     * forbit this scenario, but we still handle it.
     */
    @Test
    void backingExecutorShutdownBeforeSleepAsync() {
        Duration duration = Duration.ofMillis(SLEEP_DURATION_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseOrBackingExecutorShutdownBeforeSleepAsync(backedByExecutorService, duration, executorService::shutdownNow),
                    () -> assertCloseOrBackingExecutorShutdownBeforeSleepAsync(backedByScheduledExecutorService, duration, scheduledExecutorService::shutdownNow)
            );
        }
    }

    private static void assertCloseOrBackingExecutorShutdownBeforeSleepAsync(
            final DefaultAsyncClientExecutor clientExecutor, final Duration duration, final Runnable doBeforeSleepAsync) throws Exception {
        doBeforeSleepAsync.run();
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        clientExecutor.sleepAsync(duration, (result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
        });
        Throwable callbackException = assertThrows(CompletionException.class, () -> callbackFuture.getNow(null)).getCause();
        assertInstanceOf(RejectedExecutionException.class, callbackException);
        Thread.sleep(duration.toMillis() * 2);
        assertEquals(1, completionCount.get());
    }

    @Test
    void closeWhileSleepingInSleepAsync() {
        Duration duration = Duration.ofMillis(SLEEP_DURATION_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseWhileSleepingInSleepAsync(backedByExecutorService, duration),
                    () -> assertCloseWhileSleepingInSleepAsync(backedByScheduledExecutorService, duration)
            );
        }
    }

    private static void assertCloseWhileSleepingInSleepAsync(
            final DefaultAsyncClientExecutor clientExecutor, final Duration duration) throws Exception {
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        clientExecutor.sleepAsync(duration, (result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
        });
        clientExecutor.close();
        long timeoutMs = duration.toMillis() * 2;
        Throwable callbackException = assertThrows(ExecutionException.class, () -> callbackFuture.get(timeoutMs, MILLISECONDS)).getCause();
        assertInstanceOf(RejectedExecutionException.class, callbackException);
        Thread.sleep(timeoutMs);
        assertEquals(1, completionCount.get());
    }

    @Test
    void closeWhileCallbackIsBeingCompletedInSleepAsync() {
        Duration duration = Duration.ofMillis(SLEEP_DURATION_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseWhileCallbackIsBeingCompletedInSleepAsync(backedByExecutorService, duration),
                    () -> assertCloseWhileCallbackIsBeingCompletedInSleepAsync(backedByScheduledExecutorService, duration)
            );
        }
    }

    private static void assertCloseWhileCallbackIsBeingCompletedInSleepAsync(
            final DefaultAsyncClientExecutor clientExecutor, final Duration duration) throws Exception {
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        clientExecutor.sleepAsync(duration, (result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
            waitForCompletion(closeFuture, duration);
        });
        waitForCompletion(callbackFuture, duration.multipliedBy(2));
        clientExecutor.close();
        closeFuture.complete(null);
        long timeoutMs = duration.toMillis() * 2;
        assertDoesNotThrow(() -> callbackFuture.get(timeoutMs, MILLISECONDS));
        Thread.sleep(timeoutMs);
        assertEquals(1, completionCount.get());
    }

    private static void waitForCompletion(final Future<Void> future, final Duration duration) {
        try {
            future.get(duration.toNanos(), NANOSECONDS);
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException(null, e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // nothing to do
        }
    }
}
