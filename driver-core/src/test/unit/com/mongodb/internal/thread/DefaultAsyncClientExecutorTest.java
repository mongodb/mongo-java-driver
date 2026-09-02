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

import com.mongodb.internal.thread.AsyncClientExecutor.RejectableRunnable;
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
import static org.junit.jupiter.api.Assertions.fail;

class DefaultAsyncClientExecutorTest {
    private static final long SCHEDULE_DELAY_MILLIS = 200;

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
    @ValueSource(longs = {0, SCHEDULE_DELAY_MILLIS})
    void schedule(final long delayMs) {
        Duration delay = Duration.ofMillis(delayMs);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertSchedule(backedByExecutorService, delay),
                    () -> assertSchedule(backedByScheduledExecutorService, delay)
            );
        }
    }

    private static void assertSchedule(
            final DefaultAsyncClientExecutor clientExecutor, final Duration delay) throws Exception {
        StartTime startTime = StartTime.now();
        CompletableFuture<Duration> callbackDelayFuture = new CompletableFuture<>();
        CompletableFuture<Thread> callbackThreadFuture = new CompletableFuture<>();
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            if (t != null) {
                callbackDelayFuture.completeExceptionally(t);
            } else {
                callbackDelayFuture.complete(startTime.elapsed());
            }
            callbackThreadFuture.complete(Thread.currentThread());
        }), delay);
        long timeoutMs = delay.isZero() ? SCHEDULE_DELAY_MILLIS : delay.toMillis() * 2;
        Duration actualCallbackDelay = callbackDelayFuture.get(timeoutMs, MILLISECONDS);
        Thread actualCallbackThread = callbackThreadFuture.get(timeoutMs, MILLISECONDS);
        assertTrue(actualCallbackDelay.compareTo(delay) >= 0);
        Duration expectedMaxDelay = delay.isZero() ? Duration.ofMillis(SCHEDULE_DELAY_MILLIS) : delay.multipliedBy(2);
        assertTrue(actualCallbackDelay.compareTo(expectedMaxDelay) < 0);
        assertNotSame(Thread.currentThread(), actualCallbackThread);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, SCHEDULE_DELAY_MILLIS})
    void closeBeforeSchedule(final long delayMs) {
        Duration delay = Duration.ofMillis(delayMs);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseOrBackingExecutorShutdownBeforeSchedule(backedByExecutorService, delay, backedByExecutorService::close),
                    () -> assertCloseOrBackingExecutorShutdownBeforeSchedule(backedByScheduledExecutorService, delay, backedByScheduledExecutorService::close)
            );
        }
    }

    /**
     * {@link AsyncClientExecutor#close()} and {@link com.mongodb.connection.NettyTransportSettings.Builder#eventLoopGroup(EventLoopGroup)}
     * forbit this scenario, but we still handle it.
     */
    @Test
    void backingExecutorShutdownBeforeSchedule() {
        Duration delay = Duration.ofMillis(SCHEDULE_DELAY_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseOrBackingExecutorShutdownBeforeSchedule(backedByExecutorService, delay, executorService::shutdownNow),
                    () -> assertCloseOrBackingExecutorShutdownBeforeSchedule(backedByScheduledExecutorService, delay, scheduledExecutorService::shutdownNow)
            );
        }
    }

    private static void assertCloseOrBackingExecutorShutdownBeforeSchedule(
            final DefaultAsyncClientExecutor clientExecutor, final Duration delay, final Runnable doBeforeSchedule) throws Exception {
        doBeforeSchedule.run();
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
        }), delay);
        Throwable callbackException = assertThrows(CompletionException.class, () -> callbackFuture.getNow(null)).getCause();
        assertInstanceOf(RejectedExecutionException.class, callbackException);
        Thread.sleep(delay.isZero() ? SCHEDULE_DELAY_MILLIS : delay.toMillis() * 2);
        assertEquals(1, completionCount.get());
    }

    @Test
    void closeWhileTaskIsWaitingToBeExecutedAfterSchedule() {
        Duration delay = Duration.ofMillis(SCHEDULE_DELAY_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseWhileTaskIsWaitingToBeExecutedAfterSchedule(backedByExecutorService, delay),
                    () -> assertCloseWhileTaskIsWaitingToBeExecutedAfterSchedule(backedByScheduledExecutorService, delay)
            );
        }
    }

    private static void assertCloseWhileTaskIsWaitingToBeExecutedAfterSchedule(
            final DefaultAsyncClientExecutor clientExecutor, final Duration delay) throws Exception {
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
        }), delay);
        clientExecutor.close();
        long timeoutMs = delay.toMillis() * 2;
        Throwable callbackException = assertThrows(ExecutionException.class, () -> callbackFuture.get(timeoutMs, MILLISECONDS)).getCause();
        assertInstanceOf(RejectedExecutionException.class, callbackException);
        Thread.sleep(timeoutMs);
        assertEquals(1, completionCount.get());
    }

    @Test
    void closeWhileTaskIsBeingExecutedAfterSchedule() {
        Duration delay = Duration.ofMillis(SCHEDULE_DELAY_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseWhileTaskIsBeingExecutedAfterSchedule(backedByExecutorService, delay),
                    () -> assertCloseWhileTaskIsBeingExecutedAfterSchedule(backedByScheduledExecutorService, delay)
            );
        }
    }

    private static void assertCloseWhileTaskIsBeingExecutedAfterSchedule(
            final DefaultAsyncClientExecutor clientExecutor, final Duration delay) throws Exception {
        AtomicInteger completionCount = new AtomicInteger();
        CompletableFuture<Void> callbackFuture = new CompletableFuture<>();
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            if (t != null) {
                callbackFuture.completeExceptionally(t);
            } else {
                callbackFuture.complete(result);
            }
            waitForCompletion(closeFuture, delay);
        }), delay);
        waitForCompletion(callbackFuture, delay.multipliedBy(2));
        clientExecutor.close();
        closeFuture.complete(null);
        long timeoutMs = delay.toMillis() * 2;
        assertDoesNotThrow(() -> callbackFuture.get(timeoutMs, MILLISECONDS));
        Thread.sleep(timeoutMs);
        assertEquals(1, completionCount.get());
    }

    @Test
    void closeWhileTaskIsWaitingToBeExecutedAfterScheduleExecutesAllTasksDespiteFailures() {
        Duration delay = Duration.ofMillis(SCHEDULE_DELAY_MILLIS);
        try (DefaultAsyncClientExecutor backedByExecutorService = new DefaultAsyncClientExecutor(executorService);
             DefaultAsyncClientExecutor backedByScheduledExecutorService = new DefaultAsyncClientExecutor(scheduledExecutorService)) {
            assertAll(
                    () -> assertCloseWhileTaskIsWaitingToBeExecutedAfterScheduleExecutesAllTasksDespiteFailures(backedByExecutorService, delay),
                    () -> assertCloseWhileTaskIsWaitingToBeExecutedAfterScheduleExecutesAllTasksDespiteFailures(backedByScheduledExecutorService, delay)
            );
        }
    }

    private static void assertCloseWhileTaskIsWaitingToBeExecutedAfterScheduleExecutesAllTasksDespiteFailures(
            final DefaultAsyncClientExecutor clientExecutor, final Duration delay) {
        AtomicInteger completionCount = new AtomicInteger();
        RuntimeException exception = new RuntimeException("must not prevent task execution caused by `close`");
        Error error = new Error("must not prevent task execution caused by `close`");
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            throw exception;
        }), delay);
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            throw exception;
        }), delay);
        clientExecutor.schedule(RejectableRunnable.from((result, t) -> {
            completionCount.incrementAndGet();
            throw error;
        }), delay);
        Throwable actualThrowable = assertThrows(Throwable.class, clientExecutor::close);
        // the order of task execution caused by `close` is indeterministic, so we handle all possibilities
        if (actualThrowable instanceof RuntimeException) {
            assertSame(exception, actualThrowable);
            assertSame(error, actualThrowable.getSuppressed()[0]);
        } else if (actualThrowable instanceof Error) {
            assertSame(error, actualThrowable);
            assertSame(exception, actualThrowable.getSuppressed()[0]);
        } else {
            fail(actualThrowable);
        }
        assertEquals(3, completionCount.get());
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
