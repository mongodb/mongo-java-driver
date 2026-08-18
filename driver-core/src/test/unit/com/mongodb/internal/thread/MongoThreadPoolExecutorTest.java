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

import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoThreadPoolExecutorTest {
    private static final long TIMEOUT_MILLIS = 100;

    @Nullable
    private PrintStream originalStderr;
    private ByteArrayOutputStream stderrTap;
    @Nullable
    private UncaughtExceptionHandler originalUncaughtExceptionHandler;

    @BeforeEach
    void beforeEach() throws UnsupportedEncodingException {
        originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        originalStderr = System.err;
        stderrTap = new ByteArrayOutputStream();
        System.setErr(new PrintStream(stderrTap, true, StandardCharsets.UTF_8.name()));
    }

    @AfterEach
    void afterEach() throws IOException {
        try {
            Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler);
        } finally {
            try {
                if (originalStderr != null) {
                    System.setErr(originalStderr);
                }
            } finally {
                if (stderrTap != null) {
                    stderrTap.close();
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "false, false",
            "false, true",
            "true, false",
            "true, true"
    })
    void delegateErrorToDefaultUncaughtExceptionHandlerOrLog(
            final boolean taskCompletesAbruptlyWithError,
            final boolean setDefaultUncaughtExceptionHandler) throws Exception {
        MongoThreadPoolExecutor executor = new MongoThreadPoolExecutor(
                1, 1, Duration.ofMillis(TIMEOUT_MILLIS), new LinkedBlockingQueue<>(), new DaemonThreadFactory("test"));
        try {
            Error error = new Error("expected error");
            RuntimeException exception = new RuntimeException("expected exception");
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
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.execute(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.submit(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.submit(runnable, null));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.submit(callable));
        } finally {
            executor.shutdownNow();
        }
    }

    void assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(
            final Throwable expectedThrowable,
            final boolean setDefaultUncaughtExceptionHandler,
            final Runnable submitThrowingTask) throws Exception {
        CompletableFuture<Throwable> uncaughtExceptionFuture = new CompletableFuture<>();
        if (setDefaultUncaughtExceptionHandler) {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                uncaughtExceptionFuture.complete(e);
            });
        } else {
            // we remove the original `UncaughtExceptionHandler` to guarantee that uncaught exceptions are printed to `System.err`
            Thread.setDefaultUncaughtExceptionHandler(null);
        }
        stderrTap.reset();
        submitThrowingTask.run();
        Thread.sleep(TIMEOUT_MILLIS);
        if (setDefaultUncaughtExceptionHandler) {
            Throwable actualUncaughtException = uncaughtExceptionFuture.get(TIMEOUT_MILLIS, MILLISECONDS);
            if (expectedThrowable instanceof Error) {
                assertSame(expectedThrowable, actualUncaughtException);
            } else {
                assertInstanceOf(Error.class, actualUncaughtException);
                assertSame(expectedThrowable, actualUncaughtException.getCause());
            }
        } else {
            String actualLoggedMessage = stderrTap.toString(StandardCharsets.UTF_8.name());
            assertTrue(actualLoggedMessage.contains(expectedThrowable.getClass().getName()) && actualLoggedMessage.contains(expectedThrowable.getMessage()),
                    () -> {
                        return String.format("actualLoggedMessage=%s does not contain information about expectedThrowable=%s",
                                actualLoggedMessage, expectedThrowable);
                    });
        }
    }
}
