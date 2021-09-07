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
package com.mongodb.internal.function;

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

final class AsyncCallbackSupplierTest {
    private static final Object RESULT = new Object();

    private RuntimeException exception = new RuntimeException();
    private Error exception2 = new Error();
    @Nullable
    private Thread getThread;
    @Nullable
    private Thread finallyThread;
    private Callback callback;

    @BeforeEach
    void beforeEach() {
        callback = new Callback();
    }

    @AfterEach
    void afterEach() {
        exception = new RuntimeException();
        exception2 = new Error();
        getThread = null;
        finallyThread = null;
        callback = new Callback();
    }

    @Test
    void andFinally() {
        try {
            new PredefinedResultAsyncCallbackSupplier(RESULT)
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get(callback);
        } finally {
            callback.assertResult(RESULT);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinally2() {
        try {
            new PredefinedResultAsyncCallbackSupplier(exception)
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get(callback);
        } finally {
            callback.assertResult(exception);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyThrow() {
        try {
            new PredefinedResultAsyncCallbackSupplier(RESULT)
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                        throw exception;
                    })
                    .get(callback);
        } finally {
            callback.assertResult(exception);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyThrow2() {
        try {
            new PredefinedResultAsyncCallbackSupplier(exception)
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                        throw exception2;
                    })
                    .get(callback);
        } finally {
            callback.assertResult(exception);
            assertEquals(1, exception.getSuppressed().length);
            assertSame(exception2, exception.getSuppressed()[0]);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyCallbackThrows() {
        PredefinedResultAsyncCallbackSupplier asyncSupplier = new PredefinedResultAsyncCallbackSupplier(RESULT);
        try {
            asyncSupplier
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                        throw exception;
                    });
        } finally {
            asyncSupplier.waitForCompletion();
            assertNotNull(getThread);
            assertNotSame(Thread.currentThread(), getThread);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyCallbackThrowsSync() {
        try {
            assertThrows(exception.getClass(), () -> ((AsyncCallbackSupplier<Object>) callback ->
                    callback.onResult(RESULT, null))
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get((result, t) -> {
                        throw exception;
                    }));
        } finally {
            assertSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyThrowCallbackThrows() {
        PredefinedResultAsyncCallbackSupplier asyncSupplier = new PredefinedResultAsyncCallbackSupplier(RESULT);
        try {
            asyncSupplier
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                        throw exception;
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                        throw exception2;
                    });
        } finally {
            asyncSupplier.waitForCompletion();
            assertNotNull(getThread);
            assertNotSame(Thread.currentThread(), getThread);
            assertNotNull(finallyThread);
            assertNotSame(Thread.currentThread(), finallyThread);
        }
    }

    @Test
    void andFinallyThrowCallbackThrowsSync() {
        try {
            assertThrows(exception2.getClass(), () -> ((AsyncCallbackSupplier<Object>) callback ->
                    callback.onResult(RESULT, null))
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                        throw exception;
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                        throw exception2;
                    }));
        } finally {
            assertSame(Thread.currentThread(), finallyThread);
            assertSame(Thread.currentThread(), getThread);
        }
    }

    /**
     * If {@link AsyncCallbackSupplier#get(SingleResultCallback)} does not throw an exception and also does not complete its
     * callback, then it is impossible to execute the action supplied to {@link AsyncCallbackSupplier#andFinally(Runnable)}.
     */
    @Test
    void andFinallyGetThrows() {
        PredefinedResultAsyncCallbackSupplier asyncSupplier = new PredefinedResultAsyncCallbackSupplier(callback -> {
            throw exception;
        });
        try {
            asyncSupplier
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                    });
        } finally {
            asyncSupplier.waitForCompletion();
            assertNull(finallyThread);
            assertNull(getThread);
        }
    }

    @Test
    void andFinallyGetThrowsSync() {
        try {
            assertThrows(exception.getClass(), () -> ((AsyncCallbackSupplier<Object>) callback -> {
                        throw exception;
                    })
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                    }));
        } finally {
            assertSame(Thread.currentThread(), finallyThread);
            assertNull(getThread);
        }
    }

    @Test
    void andFinallyThrowGetThrowsSync() {
        try {
            assertThrows(exception.getClass(), () -> ((AsyncCallbackSupplier<Object>) callback -> {
                        throw exception;
                    })
                    .andFinally(() -> {
                        assertNull(finallyThread);
                        finallyThread = Thread.currentThread();
                        throw exception2;
                    })
                    .get((result, t) -> {
                        assertNull(getThread);
                        getThread = Thread.currentThread();
                    }));
        } finally {
            assertSame(Thread.currentThread(), finallyThread);
            assertNull(getThread);
            assertEquals(1, exception.getSuppressed().length);
            assertSame(exception2, exception.getSuppressed()[0]);
        }
    }

    private static final class Callback implements SingleResultCallback<Object> {
        private final CompletableFuture<Object> result = new CompletableFuture<>();

        @Override
        public void onResult(@Nullable final Object result, @Nullable final Throwable t) {
            if (t != null) {
                this.result.completeExceptionally(t);
            } else {
                this.result.complete(result);
            }
        }

        void assertResult(final Object expectedResult) {
            try {
                assertSame(expectedResult, result.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                fail(e);
            }
        }

        void assertResult(final Throwable expectedT) {
            try {
                result.get(3, TimeUnit.SECONDS);
                fail();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                assertSame(expectedT, e.getCause());
            } catch (TimeoutException e) {
                fail(e);
            }
        }
    }

    private static final class PredefinedResultAsyncCallbackSupplier implements AsyncCallbackSupplier<Object> {
        @Nullable
        private final Object result;
        @Nullable
        private final Throwable t;
        @Nullable
        private final Consumer<SingleResultCallback<Object>> get;
        @Nullable
        private Thread thread;

        PredefinedResultAsyncCallbackSupplier(final Object result) {
            this.result = result;
            this.t = null;
            get = null;
        }

        PredefinedResultAsyncCallbackSupplier(final Throwable t) {
            this.result = null;
            this.t = t;
            get = null;
        }

        PredefinedResultAsyncCallbackSupplier(final Consumer<SingleResultCallback<Object>> asyncGet) {
            this.result = null;
            this.t = null;
            this.get = asyncGet;
        }

        @Override
        public void get(final SingleResultCallback<Object> callback) {
            thread = new Thread(() -> {
                if (get == null) {
                    callback.onResult(result, t);
                } else {
                    get(callback);
                }
            });
            thread.start();
        }

        void waitForCompletion() {
            assertNotNull(thread);
            try {
                thread.join(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
