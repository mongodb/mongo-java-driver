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

package com.mongodb.internal.async;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Separate thread async functions")
public class SeparateThreadAsyncFunctionsTest extends AsyncFunctionsAbstractTest {

    private UncaughtExceptionHandler uncaughtExceptionHandler;

    @Override
    public ExecutorService createAsyncExecutor() {
        uncaughtExceptionHandler = new UncaughtExceptionHandler();
        return Executors.newFixedThreadPool(1, r -> {
            Thread thread = new Thread(r);
            thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return thread;
        });
    }

    /**
     * This test covers the scenario where a callback is erroneously invoked after a callback had been completed.
     * Such behavior is considered a bug and is not expected. An AssertionError should be thrown if an asynchronous invocation
     * attempts to use a callback that has already been marked as completed.
     */
    @Test
    void shouldPropagateAssertionErrorIfCallbackHasBeenCompletedAfterAsyncInvocation() {
        //given
        setIsTestingAbruptCompletion(false);
        setAsyncStep(true);
        IllegalStateException illegalStateException = new IllegalStateException("must not cause second callback invocation");

        AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        CompletableFuture<Void> finalCallbackWasInvoked = new CompletableFuture<>();

        //when
        beginAsync().thenRun(c -> {
                    async(3, c);
                    throw illegalStateException;
                }).thenRun(c -> {
                    assertInvokedOnce(callbackInvoked);
                    c.complete(c);
                })
                .finish((v, e) -> {
                            assertEquals(illegalStateException, e);
                            finalCallbackWasInvoked.complete(null);
                        }
                );

        //then
        Throwable exception = uncaughtExceptionHandler.getException();
        assertNotNull(exception);
        assertEquals(AssertionError.class, exception.getClass());
        assertEquals("Callback has been already completed. It could happen "
                + "if code throws an exception after invoking an async method.", exception.getMessage());
    }

    @Test
    void shouldPropagateUnexpectedExceptionFromFinishCallback() {
        //given
        setIsTestingAbruptCompletion(false);
        setAsyncStep(true);
        IllegalStateException illegalStateException = new IllegalStateException("must not cause second callback invocation");
        CompletableFuture<Void> finalCallbackWasInvoked = new CompletableFuture<>();

        //when
        beginAsync().thenRun(c -> {
            async(3, c);
        }).finish((v, e) -> {
            finalCallbackWasInvoked.complete(null);
            throw illegalStateException;
        });

        //then
        Throwable exception = uncaughtExceptionHandler.getException();
        assertNotNull(exception);
        assertEquals(illegalStateException, exception);
    }

    private static void assertInvokedOnce(final AtomicBoolean callbackInvoked1) {
        assertTrue(callbackInvoked1.compareAndSet(false, true));
    }

    private final class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final CompletableFuture<Throwable> completable = new CompletableFuture<>();

        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            completable.complete(e);
        }

        public Throwable getException() {
           return await(completable, "No exception was thrown");
        }
    }
}
