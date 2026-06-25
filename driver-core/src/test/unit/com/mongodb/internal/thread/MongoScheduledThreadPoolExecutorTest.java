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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoScheduledThreadPoolExecutorTest extends MongoThreadPoolExecutorTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Override
    void delegateErrorToDefaultUncaughtExceptionHandlerAndLog(final boolean taskCompletesAbruptlyWithError) throws Exception {
        MongoScheduledThreadPoolExecutor executor = new MongoScheduledThreadPoolExecutor(1, new DaemonThreadFactory("test"), getLogger());
        try {
            Error error = new AssertionError();
            RuntimeException exception = new RuntimeException();
            Throwable expectedException = taskCompletesAbruptlyWithError ? error : exception;
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
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.execute(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.submit(runnable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.submit(runnable, null));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.submit(callable));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.schedule(runnable, 0, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.schedule(callable, 0, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.scheduleAtFixedRate(runnable, 0, 1, MILLISECONDS));
            assertDelegateErrorToDefaultUncaughtExceptionHandlerAndLog(expectedException, false, () -> executor.scheduleWithFixedDelay(runnable, 0, 1, MILLISECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
