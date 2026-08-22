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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.mongodb.internal.thread.CommonExecutor.commonExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class CommonExecutorTest {
    private static final long TIMEOUT_MILLIS = 400;
    /**
     * This test verifies that even in the unlikely event that the single scheduling thread is terminated,
     * it is replaced with another one to execute a previously scheduled task.
     */
    @Test
    void singleSchedulingThreadIsReplacedIfTerminated() throws Exception {
        Executor sameThreadExecutor = Runnable::run;
        CompletableFuture<Thread> newSchedulingThread = new CompletableFuture<>();
        commonExecutor().schedule(
                () -> newSchedulingThread.complete(Thread.currentThread()),
                Duration.ofMillis(TIMEOUT_MILLIS / 2),
                sameThreadExecutor);
        CompletableFuture<Thread> terminatedSchedulingThread = new CompletableFuture<>();
        commonExecutor().schedule(
                () -> {
                    terminatedSchedulingThread.complete(Thread.currentThread());
                    throw new Error("This error is thrown in the single scheduling thread, causing its termination");
                },
                Duration.ZERO,
                sameThreadExecutor);
        assertNotSame(terminatedSchedulingThread.get(TIMEOUT_MILLIS, MILLISECONDS), newSchedulingThread.get(TIMEOUT_MILLIS, MILLISECONDS));
    }
}
