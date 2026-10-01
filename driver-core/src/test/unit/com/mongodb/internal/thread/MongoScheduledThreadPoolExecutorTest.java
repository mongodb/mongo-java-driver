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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoScheduledThreadPoolExecutorTest extends MongoThreadPoolExecutorTest {
    @ParameterizedTest
    @CsvSource({
            "false, false",
            "false, true",
            "true, false",
            "true, true"})
    @Override
    void delegateErrorToDefaultUncaughtExceptionHandlerOrLog(
            final boolean taskCompletesAbruptlyWithError,
            final boolean setDefaultUncaughtExceptionHandler) throws Exception {
        MongoScheduledThreadPoolExecutor executor = new MongoScheduledThreadPoolExecutor(1, new DaemonThreadFactory("test"));
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
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.schedule(runnable, 0, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.schedule(callable, 0, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.scheduleAtFixedRate(runnable, 0, 1, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerOrLog(expectedThrowable, setDefaultUncaughtExceptionHandler, () -> executor.scheduleWithFixedDelay(runnable, 0, 1, MILLISECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
