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

import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.mockito.MongoMockito;
import com.mongodb.internal.thread.AsyncClientExecutor.RejectableRunnable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;

class ThreadUtilTest {
    @Test
    void sleepAsyncCompletesInCallingThreadIfNoDelay() {
        AsyncClientExecutor failingClientExecutor = MongoMockito.mock(AsyncClientExecutor.class);
        Thread expectedThread = Thread.currentThread();
        // we use `AtomicReference` instead of `MutableValue` in case completion incorrectly happens in a different thread
        AtomicReference<Thread> actualThread = new AtomicReference<>();
        AtomicReference<Throwable> actualThrowable = new AtomicReference<>();
        ThreadUtil.sleepAsync(Duration.ZERO, failingClientExecutor, (result, t) -> {
            actualThread.set(Thread.currentThread());
            actualThrowable.set(t);
        });
        assertSame(expectedThread, actualThread.get());
        assertNull(actualThrowable.get());
    }

    @Test
    void sleepAsyncSchedulesIfDelay() {
        RejectedExecutionException expectedRejectionCauseFromClientExecutor = new RejectedExecutionException();
        Duration delay = Duration.ofNanos(1);
        AsyncClientExecutor clientExecutor = MongoMockito.mock(AsyncClientExecutor.class, mock -> {
            doAnswer(invocation -> {
                RejectableRunnable task = invocation.getArgument(0);
                task.reject(expectedRejectionCauseFromClientExecutor);
                return null;
            }).when(mock).schedule(any(), same(delay));
        });
        MutableValue<Object> completedByClientExecutor = new MutableValue<>();
        ThreadUtil.sleepAsync(delay, clientExecutor, (result, t) -> {
            completedByClientExecutor.set(t);
        });
        assertSame(expectedRejectionCauseFromClientExecutor, completedByClientExecutor.getNullable());
    }
}
