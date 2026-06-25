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

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class MongoThreadPoolExecutorTest {
    private static final long TIMEOUT_MILLIS = 100;

    private Logger logger;

    @Nullable
    private UncaughtExceptionHandler originalUncaughtExceptionHandler;

    @BeforeEach
    void beforeEach() {
        logger = spy(Loggers.getLogger("test"));
        originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    }

    @AfterEach
    void afterEach() {
        if (originalUncaughtExceptionHandler != null) {
            Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delegateErrorToDefaultUncaughtExceptionHandlerAndLog(final boolean taskCompletesAbruptlyWithError) throws Exception {
        MongoThreadPoolExecutor executor = new MongoThreadPoolExecutor(
                1, 1, Duration.ofMillis(TIMEOUT_MILLIS), new LinkedBlockingQueue<>(), new DaemonThreadFactory("test"), logger);
        try {
            Error error = new AssertionError();
            RuntimeException exception = new RuntimeException();
            Throwable expectedThrowable = taskCompletesAbruptlyWithError ? error : exception;
            Runnable runnable = () -> {
                if (taskCompletesAbruptlyWithError) {
                    throw error;
                } else {
                    throw exception;
                }
            };
            Callable<Void> callable = () -> {
                runnable.run();
                return null;
            };
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedThrowable, true, () -> executor.execute(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedThrowable, false, () -> executor.submit(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedThrowable, false, () -> executor.submit(runnable, null));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedThrowable, false, () -> executor.submit(callable));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * @param delegatesExceptionToUncaughtExceptionHandler The {@link ExecutorService#execute(Runnable)} method usually results in
     * the task exception to be thrown from the worker thread {@link Thread#run()} method,
     * though {@link ScheduledThreadPoolExecutor#execute(Runnable)} is an example where that is not the case.
     * This is different from the behavior of the {@link ExecutorService#submit(Runnable)} method, and similar methods that return
     * a {@link Future} holding the task exception.
     * This parameter reflects the expected behavior, depending on the method used by {@code submitThrowingTask}.
     */
    void assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(
            final Throwable expectedThrowable,
            final boolean delegatesExceptionToUncaughtExceptionHandler,
            final Runnable submitThrowingTask) throws Exception {
        CompletableFuture<Throwable> uncaughtExceptionFuture = new CompletableFuture<>();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            uncaughtExceptionFuture.complete(e);
        });
        clearInvocations(logger);
        submitThrowingTask.run();
        Thread.sleep(TIMEOUT_MILLIS);
        if (expectedThrowable instanceof Error || delegatesExceptionToUncaughtExceptionHandler) {
            Throwable actualUncaughtException = uncaughtExceptionFuture.get(TIMEOUT_MILLIS, MILLISECONDS);
            assertSame(expectedThrowable, actualUncaughtException);
        } else {
            assertFalse(uncaughtExceptionFuture.isDone());
        }
        verify(logger).error(matches("A task completed abruptly"), any());
    }

    Logger getLogger() {
        return logger;
    }
}
