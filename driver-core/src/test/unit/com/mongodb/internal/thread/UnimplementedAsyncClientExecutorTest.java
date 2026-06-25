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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnimplementedAsyncClientExecutorTest {
    @ParameterizedTest
    @ValueSource(longs = {0, 200})
    void sleepAsync(final long durationMs) {
        Duration duration = Duration.ofMillis(durationMs);
        assertSleepAsync(duration);
    }

    private static void assertSleepAsync(final Duration duration) {
        CompletableFuture<Void> callbackExceptionFuture = new CompletableFuture<>();
        CompletableFuture<Thread> callbackThreadFuture = new CompletableFuture<>();
        try (UnimplementedAsyncClientExecutor unimplementedClientExecutor = UnimplementedAsyncClientExecutor.instance()) {
            unimplementedClientExecutor.sleepAsync(duration, (result, t) -> {
                if (t != null) {
                    callbackExceptionFuture.completeExceptionally(t);
                } else {
                    callbackExceptionFuture.complete(result);
                }
                callbackThreadFuture.complete(Thread.currentThread());
            });
        }
        if (duration.isZero()) {
            assertDoesNotThrow(() -> callbackExceptionFuture.getNow(null));
        } else {
            Throwable callbackException = assertThrows(CompletionException.class, () -> callbackExceptionFuture.getNow(null)).getCause();
            assertSame(AssertionError.class, callbackException.getClass());
        }
        Thread actualCallbackThread = callbackThreadFuture.getNow(null);
        assertSame(Thread.currentThread(), actualCallbackThread);
    }
}
