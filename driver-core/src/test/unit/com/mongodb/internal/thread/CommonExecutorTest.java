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
import org.junit.jupiter.api.Test;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CompletableFuture;

import static com.mongodb.internal.thread.CommonExecutor.commonExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class CommonExecutorTest {
    private static final long TIMEOUT_MILLIS = SECONDS.toMillis(1);

    @Nullable
    private UncaughtExceptionHandler originalUncaughtExceptionHandler;

    @BeforeEach
    void beforeEach() {
        originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    }

    @AfterEach
    void afterEach() {
        if (originalUncaughtExceptionHandler != null) {
            Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler);
        }
    }

    @Test
    void uncaughtExceptionHandlerExecutesInDifferentThread() throws Exception {
        CompletableFuture<Thread> threadFuture = new CompletableFuture<>();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            threadFuture.complete(Thread.currentThread());
        });
        commonExecutor().uncaughtError(new AssertionError());
        assertNotSame(Thread.currentThread(), threadFuture.get(TIMEOUT_MILLIS, MILLISECONDS));
    }
}
