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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

final class AsyncRunnableTest {
    private final AtomicInteger i = new AtomicInteger();

    @Test
    void testRunnableRun() {
        /*
        In our async code:
        1. a callback is provided
        2. at least one sync method must be converted to async

        To do this:
        1. start an async chain using the static method
        2. chain using the appropriate method, which will provide "c"
        3. move all sync code into that method
        4. at the async method, pass in "c" and start a new chained method
        5. complete by invoking the original "callback" at the end of the chain

        Async methods may be preceded by "unaffected" sync code, and this code
        will reside above the affected method, as it appears in the sync code.
        Below, these "unaffected" methods have no sync/async suffix.

        The return of each chained async method MUST be immediately preceded
        by an invocation of the relevant async method using "c".

        Always use a braced lambda body to ensure that the form matches the
        corresponding sync code.
        */
        assertBehavesSame(
                () -> {
                    multiply();
                    incrementSync();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableRunSyncException() {
        // Preceding sync code might throw an exception, so it SHOULD be moved
        // into the chain. In any case, any possible exception thrown by it
        // MUST be handled by passing it into the callback.
        assertBehavesSame(
                () -> {
                    throwException("msg");
                    incrementSync();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwException("msg");
                        incrementAsync(c);
                    }).finish(callback);
                });

    }

    @Test
    void testRunnableRunMultiple() {
        // Code split across multiple affected methods:
        assertBehavesSame(
                () -> {
                    multiply();
                    incrementSync();
                    multiply();
                    incrementSync();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableRunMultipleExceptionSkipping() {
        // An exception in sync code causes ensuing code to be skipped, and
        // split async code behaves in the same way:
        assertBehavesSame(
                () -> {
                    throwException("m");
                    incrementSync();
                    throwException("m2");
                    incrementSync();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwException("m");
                        incrementAsync(c);
                    }).thenRun(c -> {
                        throwException("m2");
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableRunMultipleExceptionInAffectedSkipping() {
        // Likewise, an exception in the affected method causes a skip:
        assertBehavesSame(
                () -> {
                    multiply();
                    throwExceptionSync("msg");
                    multiply();
                    incrementSync();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        throwExceptionAsync("msg", c);
                    }).thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableCompleteRunnable() {
        // Sometimes, sync code follows the affected method, and it MUST be
        // moved into the final method:
        assertBehavesSame(
                () -> {
                    incrementSync();
                    multiply();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        incrementAsync(c);
                    }).thenRunAndFinish(() -> {
                        multiply();
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteRunnableExceptional() {
        // ...this makes it easier to correctly handle its exceptions:
        assertBehavesSame(
                () -> {
                    incrementSync();
                    throwException("m");
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        incrementAsync(c);
                    }).thenRunAndFinish(() -> {
                        throwException("m");
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteRunnableSkippedWhenExceptional() {
        // ...and to ensure that it is not executed when it should be skipped:
        assertBehavesSame(
                () -> {
                    throwExceptionSync("msg");
                    multiply();
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("msg", c);
                    }).thenRunAndFinish(() -> {
                        multiply();
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteAlways() {
        // normal flow
        assertBehavesSame(
                () -> {
                    try {
                        multiply();
                        incrementSync();
                    } finally {
                        multiply();
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).thenAlwaysRunAndFinish(() -> {
                        multiply();
                    }, callback);
                });

    }

    @Test
    void testRunnableCompleteAlwaysExceptionInAffected() {
        // exception in sync/async
        assertBehavesSame(
                () -> {
                    try {
                        multiply();
                        throwExceptionSync("msg");
                    } finally {
                        multiply();
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        throwExceptionAsync("msg", c);
                    }).thenAlwaysRunAndFinish(() -> {
                        multiply();
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteAlwaysExceptionInUnaffected() {
        // exception in unaffected code
        assertBehavesSame(
                () -> {
                    try {
                        throwException("msg");
                        incrementSync();
                    } finally {
                        multiply();
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwException("msg");
                        incrementAsync(c);
                    }).thenAlwaysRunAndFinish(() -> {
                        multiply();
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteAlwaysExceptionInFinally() {
        // exception in finally
        assertBehavesSame(
                () -> {
                    try {
                        multiply();
                        incrementSync();
                    } finally {
                        throwException("msg");
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        multiply();
                        incrementAsync(c);
                    }).thenAlwaysRunAndFinish(() -> {
                        throwException("msg");
                    }, callback);
                });
    }

    @Test
    void testRunnableCompleteAlwaysExceptionInFinallyExceptional() {
        // exception in finally, exceptional flow
        assertBehavesSame(
                () -> {
                    try {
                        throwException("first");
                        incrementSync();
                    } finally {
                        throwException("msg");
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwException("first");
                        incrementAsync(c);
                    }).thenAlwaysRunAndFinish(() -> {
                        throwException("msg");
                    }, callback);
                });
    }

    @Test
    void testRunnableSupply() {
        assertBehavesSame(
                () -> {
                    multiply();
                    return valueSync(1);
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        multiply();
                        valueAsync(1, c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableSupplyExceptional() {
        assertBehavesSame(
                () -> {
                    throwException("msg");
                    return valueSync(1);
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        throwException("msg");
                        valueAsync(1, c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableSupplyExceptionalInAffected() {
        assertBehavesSame(
                () -> {
                    throwExceptionSync("msg");
                    return valueSync(1);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("msg", c);
                    }).<Integer>thenSupply(c -> {
                        valueAsync(1, c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplierOnErrorIf() {
        // no exception
        assertBehavesSame(
                () -> {
                    try {
                        return valueSync(1);
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            return valueSync(2);
                        } else {
                            throw e;
                        }
                    }
                },
                (SingleResultCallback<Integer> callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        valueAsync(1, c);
                    }).onErrorSupplyIf(e -> e.getMessage().equals("m1"), c -> {
                        valueAsync(2, c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplierOnErrorIfWithValueBranch() {
        // exception, with value branch
        assertBehavesSame(
                () -> {
                    try {
                        return throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            return valueSync(2);
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorSupplyIf(e -> e.getMessage().equals("m1"), c -> {
                        valueAsync(2, c);
                    }).finish(callback);
                });

    }

    @Test
    void testSupplierOnErrorIfWithExceptionBranch() {
        // exception, with exception branch
        assertBehavesSame(
                () -> {
                    try {
                        return throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            return this.<Integer>throwExceptionSync("m2");
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorSupplyIf(e -> e.getMessage().equals("m1"), c -> {
                        throwExceptionAsync("m2", c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableOnErrorIfNoException() {
        // no exception
        assertBehavesSame(
                () -> {
                    try {
                        incrementSync();
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            multiply();
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        incrementSync();
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });

    }

    @Test
    void testRunnableOnErrorIfThrowsMatching() {
        // throws matching exception
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            multiply();
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });

    }

    @Test
    void testRunnableOnErrorIfThrowsNonMatching() {
        // throws non-matching exception
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("not-m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            multiply();
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("not-m1", c);
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableOnErrorIfCheckFails() {
        // throws but check fails with exception
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (throwException("check fails")) {
                            multiply();
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorRunIf(e -> throwException("check fails"), c -> {
                        multiply();
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableOnErrorIfSyncBranchfails() {
        // throws but sync code in branch fails
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            throwException("branch");
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        throwException("branch");
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableOnErrorIfSyncBranchFailsWithMatching() {
        // throws but sync code in branch fails with matching exception
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            multiply();
                            throwException("m1");
                            incrementSync();
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        multiply();
                        throwException("m1");
                        incrementAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRunnableOnErrorIfThrowsAndBranchedAffectedMethodThrows() {
        // throws, and branch sync/async method throws
        assertBehavesSame(
                () -> {
                    try {
                        throwExceptionSync("m1");
                    } catch (Exception e) {
                        if (e.getMessage().equals("m1")) {
                            multiply();
                            throwExceptionSync("m1");
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        throwExceptionAsync("m1", c);
                    }).onErrorRunIf(e -> e.getMessage().equals("m1"), c -> {
                        multiply();
                        throwExceptionAsync("m1", c);
                    }).finish(callback);
                });
    }

    // unaffected methods:

    private <T> T throwException(final String message) {
        throw new RuntimeException(message);
    }

    private void multiply() {
        i.set(i.get() * 10);
    }

    // affected sync-async pairs:

    private void incrementSync() {
        i.addAndGet(1);
    }

    private void incrementAsync(final SingleResultCallback<Void> callback) {
        i.addAndGet(1);
        callback.onResult(null, null);
    }

    private <T> T throwExceptionSync(final String msg) {
        throw new RuntimeException(msg);
    }

    private <T> void throwExceptionAsync(final String msg, final SingleResultCallback<T> callback) {
        try {
            throw new RuntimeException(msg);
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    private Integer valueSync(final int i) {
        return i;
    }

    private void valueAsync(final int i, final SingleResultCallback<Integer> callback) {
        callback.onResult(i, null);
    }

    private void assertBehavesSame(final Runnable sync, final Consumer<SingleResultCallback<Void>> async) {
        assertBehavesSame(
                () -> {
                    sync.run();
                    return null;
                },
                (c) -> {
                    async.accept((v, e) -> c.onResult(v, e));
                });
    }

    private <T> void assertBehavesSame(final Supplier<T> sync, final Consumer<SingleResultCallback<T>> async) {
        AtomicReference<T> actualValue = new AtomicReference<>();
        AtomicReference<Throwable> actualException = new AtomicReference<>();
        try {
            i.set(1);
            SingleResultCallback<T> callback = (v, e) -> {
                actualValue.set(v);
                actualException.set(e);
            };
            async.accept(callback);
        } catch (Exception e) {
            fail("async threw an exception instead of using callback");
        }
        Integer expectedI = i.get();

        try {
            i.set(1);
            T expectedValue = sync.get();
            assertEquals(expectedValue, actualValue.get());
            assertNull(actualException.get());
        } catch (Exception e) {
            assertNull(actualValue.get());
            assertNotNull(actualException.get(), "async failed to throw expected: " + e);
            assertEquals(e.getClass(), actualException.get().getClass());
            assertEquals(e.getMessage(), actualException.get().getMessage());
        }
        assertEquals(expectedI, i.get());
    }
}
