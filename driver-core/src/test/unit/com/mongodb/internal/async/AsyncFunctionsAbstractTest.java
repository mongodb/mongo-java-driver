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

import com.mongodb.MongoException;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

abstract class AsyncFunctionsAbstractTest extends AsyncFunctionsTestBase {
    private static final TimeoutContext TIMEOUT_CONTEXT = new TimeoutContext(new TimeoutSettings(0, 0, 0, 0L, 0));

    @Test
    void test1Method() {
        // the number of expected variations is often: 1 + N methods invoked
        // 1 variation with no exceptions, and N per an exception in each method
        assertBehavesSameVariations(2,
                () -> {
                    // single sync method invocations...
                    sync(1);
                },
                (callback) -> {
                    // ...become a single async invocation, wrapped in begin-thenRun/finish:
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).finish(callback);
                });
    }

    @Test
    void test2Methods() {
        // tests pairs, converting: plain-sync, sync-plain, sync-sync
        // (plain-plain does not need an async chain)

        assertBehavesSameVariations(3,
                () -> {
                    // plain (unaffected) invocations...
                    plain(1);
                    sync(2);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        // ...are preserved above affected methods
                        plain(1);
                        async(2, c);
                    }).finish(callback);
                });

        assertBehavesSameVariations(3,
                () -> {
                    // when a plain invocation follows an affected method...
                    sync(1);
                    plain(2);
                },
                (callback) -> {
                    // ...it is moved to its own block, and must be completed:
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        plain(2);
                        c.complete(c);
                    }).finish(callback);
                });

        assertBehavesSameVariations(3,
                () -> {
                    // when an affected method follows an affected method
                    sync(1);
                    sync(2);
                },
                (callback) -> {
                    // ...it is moved to its own block
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        async(2, c);
                    }).finish(callback);
                });
    }

    @Test
    void test4Methods() {
        // tests the sync-sync pair with preceding and ensuing plain methods.

        assertBehavesSameVariations(5,
                () -> {
                    plain(11);
                    sync(1);
                    plain(22);
                    sync(2);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(11);
                        async(1, c);
                    }).thenRun(c -> {
                        plain(22);
                        async(2, c);
                    }).finish(callback);
                });

        assertBehavesSameVariations(5,
                () -> {
                    sync(1);
                    plain(11);
                    sync(2);
                    plain(22);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        plain(11);
                        async(2, c);
                    }).thenRunAndFinish(() ->{
                        plain(22);
                    }, callback);
                });
    }

    @Test
    void testSupply() {
        assertBehavesSameVariations(4,
                () -> {
                    sync(0);
                    plain(1);
                    return syncReturns(2);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).<Integer>thenSupply(c -> {
                        plain(1);
                        asyncReturns(2, c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyWithMixedReturns() {
        assertBehavesSameVariations(5,
                () -> {
                    if (plainTest(1)) {
                        return syncReturns(11);
                    } else {
                        return plainReturns(22);
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        if (plainTest(1)) {
                            asyncReturns(11, c);
                        } else {
                            int r = plainReturns(22);
                            c.complete(r); // corresponds to a return, and
                            // must be followed by a return or end of method
                        }
                    }).finish(callback);
                });
    }

    @Test
    void testFullChain() {
        // tests a chain with: runnable, producer, function, function, consumer
        assertBehavesSameVariations(14,
                () -> {
                    plain(90);
                    sync(0);
                    plain(91);
                    sync(1);
                    plain(92);
                    int v = syncReturns(2);
                    plain(93);
                    v = syncReturns(v + 1);
                    plain(94);
                    v = syncReturns(v + 10);
                    plain(95);
                    sync(v + 100);
                    plain(96);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(90);
                        async(0, c);
                    }).thenRun(c -> {
                        plain(91);
                        async(1, c);
                    }).<Integer>thenSupply(c -> {
                        plain(92);
                        asyncReturns(2, c);
                    }).<Integer>thenApply((v, c) -> {
                        plain(93);
                        asyncReturns(v + 1, c);
                    }).<Integer>thenApply((v, c) -> {
                        plain(94);
                        asyncReturns(v + 10, c);
                    }).thenConsume((v, c) -> {
                        plain(95);
                        async(v + 100, c);
                    }).thenRunAndFinish(() -> {
                        plain(96);
                    }, callback);
                });
    }

    @Test
    void testConditionals() {
        assertBehavesSameVariations(5,
                () -> {
                    if (plainTest(1)) {
                        sync(2);
                    } else {
                        sync(3);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        if (plainTest(1)) {
                            async(2, c);
                        } else {
                            async(3, c);
                        }
                    }).finish(callback);
                });

        // 2 : fail on first sync, fail on test
        // 3 : true test, sync2, sync3
        // 2 : false test, sync3
        // 7 total
        assertBehavesSameVariations(7,
                () -> {
                    sync(0);
                    if (plainTest(1)) {
                        sync(2);
                    }
                    sync(3);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).thenRunIf(() -> plainTest(1), c -> {
                        async(2, c);
                    }).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });

        // an additional affected method within the "if" branch
        assertBehavesSameVariations(8,
                () -> {
                    sync(0);
                    if (plainTest(1)) {
                        sync(21);
                        sync(22);
                    }
                    sync(3);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).thenRunIf(() -> plainTest(1),
                        beginAsync().thenRun(c -> {
                            async(21, c);
                        }).thenRun((c) -> {
                            async(22, c);
                        })
                    ).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });

        // empty `else` branch
        assertBehavesSameVariations(5,
                () -> {
                    if (plainTest(1)) {
                        Integer connection = syncReturns(2);
                        sync(connection + 5);
                    } else {
                        // do nothing
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        if (plainTest(1)) {
                            beginAsync().<Integer>thenSupply(c2 -> {
                                asyncReturns(2, c2);
                            }).thenConsume((connection, c3) -> {
                                async(connection + 5, c3);
                            }).finish(c);
                        } else {
                            c.complete(c); // do nothing
                        }
                    }).finish(callback);
                });
    }

    @Test
    void testMixedConditionalCascade() {
        assertBehavesSameVariations(9,
                () -> {
                    boolean test1 = plainTest(1);
                    if (test1) {
                        return syncReturns(11);
                    }
                    boolean test2 = plainTest(2);
                    if (test2) {
                        return 22;
                    }
                    int x = syncReturns(33);
                    plain(x + 100);
                    return syncReturns(44);
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        boolean test1 = plainTest(1);
                        if (test1) {
                            asyncReturns(11, c);
                            return;
                        }
                        boolean test2 = plainTest(2);
                        if (test2) {
                            c.complete(22);
                            return;
                        }
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(33, c2);
                        }).<Integer>thenApply((x, c2) -> {
                            plain(assertNotNull(x) + 100);
                            asyncReturns(44, c2);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testPlain() {
        // For completeness. This should not be used, since there is no async.
        assertBehavesSameVariations(2,
                () -> {
                    plain(1);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(1);
                        c.complete(c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatch() {
        // single method in both try and catch
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        sync(2);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        async(2, c);
                    }).finish(callback);
                });

        // mixed sync/plain
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        plain(2);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        plain(2);
                        c.complete(c);
                    }).finish(callback);
                });

        // chain of 2 in try.
        // WARNING: "onErrorIf" will consider everything in
        // the preceding chain to be part of the try.
        // Use nested async chains, or convenience methods,
        // to define the beginning of the try.
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        sync(1);
                        sync(2);
                    } catch (Throwable t) {
                        sync(9);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        async(2, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        async(9, c);
                    }).finish(callback);
                });

        // chain of 2 in catch
        assertBehavesSameVariations(4,
                () -> {
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        sync(8);
                        sync(9);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).onErrorIf(t -> true, (t, callback2) -> {
                        beginAsync().thenRun(c -> {
                            async(8, c);
                        }).thenRun(c -> {
                            async(9, c);
                        }).finish(callback2);
                    }).finish(callback);
                });

        // method after the try-catch block
        // here, the try-catch must be nested (as a code block)
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        sync(2);
                    }
                    sync(3);
                },
                (callback) -> {
                    beginAsync().thenRun(c2 -> {
                        beginAsync().thenRun(c -> {
                            async(1, c);
                        }).onErrorIf(t -> true, (t, c) -> {
                            async(2, c);
                        }).finish(c2);
                    }).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });

        // multiple catch blocks
        // WARNING: these are not exclusive; if multiple "onErrorIf" blocks
        // match, they will all be executed.
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        if (plainTest(1)) {
                            throw new UnsupportedOperationException("A");
                        } else {
                            throw new IllegalStateException("B");
                        }
                    } catch (UnsupportedOperationException t) {
                        sync(8);
                    } catch (IllegalStateException t) {
                        sync(9);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        if (plainTest(1)) {
                            throw new UnsupportedOperationException("A");
                        } else {
                            throw new IllegalStateException("B");
                        }
                    }).onErrorIf(t -> t instanceof UnsupportedOperationException, (t, c) -> {
                        async(8, c);
                    }).onErrorIf(t -> t instanceof IllegalStateException, (t, c) -> {
                        async(9, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithEmptyCatch() {
        assertBehavesSameVariations(2,
                () -> {
                    try {
                        throw new RuntimeException();
                    } catch (MongoException e) {
                        // ignore exceptions
                    } finally {
                        plain(2);
                    }
                    plain(3);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        beginAsync().thenRunTryCatchAsyncBlocks(c2 -> {
                            c2.completeExceptionally(new RuntimeException());
                        }, MongoException.class, (e, c3) -> {
                            c3.complete(c3); // ignore exceptions
                        })
                        .thenAlwaysRunAndFinish(() -> {
                            plain(2);
                        }, c);
                    }).thenRun(c4 -> {
                        plain(3);
                        c4.complete(c4);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatchHelper() {
        assertBehavesSameVariations(4,
                () -> {
                    plain(0);
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        plain(2);
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(0);
                        c.complete(c);
                    }).thenRunTryCatchAsyncBlocks(c -> {
                        async(1, c);
                    }, Throwable.class, (t, c) -> {
                        plain(2);
                        c.completeExceptionally(t);
                    }).finish(callback);
                });

        assertBehavesSameVariations(5,
                () -> {
                    plain(0);
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        plain(2);
                        throw t;
                    }
                    sync(4);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(0);
                        c.complete(c);
                    }).thenRunTryCatchAsyncBlocks(c -> {
                        async(1, c);
                    }, Throwable.class, (t, c) -> {
                        plain(2);
                        c.completeExceptionally(t);
                    }).thenRun(c -> {
                        async(4, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatchWithVariables() {
        // using supply etc.
        assertBehavesSameVariations(12,
                () -> {
                    try {
                        int i = plainTest(0) ? 1 : 2;
                        i = syncReturns(i + 10);
                        sync(i + 100);
                    } catch (Throwable t) {
                        sync(3);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(
                            beginAsync().<Integer>thenSupply(c -> {
                                int i = plainTest(0) ? 1 : 2;
                                asyncReturns(i + 10, c);
                            }).thenConsume((i, c) -> {
                                async(assertNotNull(i) + 100, c);
                            })
                    ).onErrorIf(t -> true, (t, c) -> {
                        async(3, c);
                    }).finish(callback);
                });

        // using an externally-declared variable
        assertBehavesSameVariations(17,
                () -> {
                    int i = plainTest(0) ? 1 : 2;
                    try {
                        i = syncReturns(i + 10);
                        sync(i + 100);
                    } catch (Throwable t) {
                        sync(3);
                    }
                    sync(i + 1000);
                },
                (callback) -> {
                    final int[] i = new int[1];
                    beginAsync().thenRun(c -> {
                        i[0] = plainTest(0) ? 1 : 2;
                        c.complete(c);
                    }).thenRun(c -> {
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(i[0] + 10, c2);
                        }).thenConsume((i2, c2) -> {
                            i[0] = assertNotNull(i2);
                            async(i2 + 100, c2);
                        }).onErrorIf(t -> true, (t, c2) -> {
                            async(3, c2);
                        }).finish(c);
                    }).thenRun(c -> {
                        async(i[0] + 1000, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatchWithConditionInCatch() {
        assertBehavesSameVariations(12,
                () -> {
                    try {
                        sync(plainTest(0) ? 1 : 2);
                        sync(3);
                    } catch (Throwable t) {
                        sync(5);
                        if (t.getMessage().equals("exception-1")) {
                            throw t;
                        } else {
                            throw new RuntimeException("wrapped-" + t.getMessage(), t);
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(plainTest(0) ? 1 : 2, c);
                    }).thenRun(c -> {
                        async(3, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        beginAsync().thenRun(c2 -> {
                            async(5, c2);
                        }).thenRun(c2 -> {
                            if (assertNotNull(t).getMessage().equals("exception-1")) {
                                throw (RuntimeException) t;
                            } else {
                                throw new RuntimeException("wrapped-" + t.getMessage(), t);
                            }
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatchTestAndRethrow() {
        // thenSupply:
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        return syncReturns(1);
                    } catch (Exception e) {
                        if (e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1")) {
                            return syncReturns(2);
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).onErrorIf(e -> e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1"), (t, c) -> {
                        asyncReturns(2, c);
                    }).finish(callback);
                });

        // thenRun:
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        sync(1);
                    } catch (Exception e) {
                        if (e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1")) {
                            sync(2);
                        } else {
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).onErrorIf(e -> e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1"), (t, c) -> {
                        async(2, c);
                    }).finish(callback);
                });
    }

    @Test
    void testRetryLoop() {
        assertBehavesSameVariations(InvocationTracker.DEPTH_LIMIT * 2 + 1,
                () -> {
                    while (true) {
                        try {
                            sync(plainTest(0) ? 1 : 2);
                        } catch (RuntimeException e) {
                            if (e.getMessage().equals("exception-1")) {
                                continue;
                            }
                            throw e;
                        }
                        break;
                    }
                },
                (callback) -> {
                    beginAsync().thenRunRetryingWhile(
                            TIMEOUT_CONTEXT,
                            c -> async(plainTest(0) ? 1 : 2, c),
                            e -> e.getMessage().equals("exception-1")
                    ).finish(callback);
                });
    }

    @Test
    void testFinallyWithPlainInsideTry() {
        // (in try: normal flow + exception + exception) * (in finally: normal + exception) = 6
        assertBehavesSameVariations(6,
                () -> {
                    try {
                        plain(1);
                        sync(2);
                    } finally {
                        plain(3);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(1);
                        async(2, c);
                    }).thenAlwaysRunAndFinish(() -> {
                        plain(3);
                    }, callback);
                });
    }

    @Test
    void testFinallyWithPlainOutsideTry() {
        assertBehavesSameVariations(5,
                () -> {
                    plain(1);
                    try {
                        sync(2);
                    } finally {
                        plain(3);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        plain(1);
                        beginAsync().thenRun(c2 -> {
                            async(2, c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            plain(3);
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testUsedAsLambda() {
        assertBehavesSameVariations(4,
                () -> {
                    Supplier<Integer> s = () -> syncReturns(9);
                    sync(0);
                    plain(1);
                    return s.get();
                },
                (callback) -> {
                    AsyncSupplier<Integer> s = (c) -> asyncReturns(9, c);
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).<Integer>thenSupply((c) -> {
                        plain(1);
                        s.getAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testVariables() {
        assertBehavesSameVariations(3,
                () -> {
                    int something;
                    something = 90;
                    sync(something);
                    something = something + 10;
                    sync(something);
                },
                (callback) -> {
                    // Certain variables may need to be shared; these can be
                    // declared (but not initialized) outside the async chain.
                    // Any container works (atomic allowed but not needed)
                    final int[] something = new int[1];
                    beginAsync().thenRun(c -> {
                        something[0] = 90;
                        async(something[0], c);
                    }).thenRun((c) -> {
                        something[0] = something[0] + 10;
                        async(something[0], c);
                    }).finish(callback);
                });
    }

    @Test
    void testDerivation() {
        // Demonstrates the progression from nested async to the API.

        // Stand-ins for sync-async methods; these "happily" do not throw
        // exceptions, to avoid complicating this demo async code.
        Consumer<Integer> happySync = (i) -> {
            getNextOption(1);
            listenerAdd("affected-success-" + i);
        };
        BiConsumer<Integer, SingleResultCallback<Void>> happyAsync = (i, c) -> {
            happySync.accept(i);
            c.complete(c);
        };

        // Standard nested async, no error handling:
        assertBehavesSameVariations(1,
                () -> {
                    happySync.accept(1);
                    happySync.accept(2);
                },
                (callback) -> {
                    happyAsync.accept(1, (v, e) -> {
                        happyAsync.accept(2, callback);
                    });
                });

        // When both methods are naively extracted, they are out of order:
        assertBehavesSameVariations(1,
                () -> {
                    happySync.accept(1);
                    happySync.accept(2);
                },
                (callback) -> {
                    SingleResultCallback<Void> second = (v, e) -> {
                        happyAsync.accept(2, callback);
                    };
                    SingleResultCallback<Void> first = (v, e) -> {
                        happyAsync.accept(1, second);
                    };
                    first.onResult(null, null);
                });

        // We create an "AsyncRunnable" that takes a callback, which
        // decouples any async methods from each other, allowing them
        // to be declared in a sync-like order, and without nesting:
        assertBehavesSameVariations(1,
                () -> {
                    happySync.accept(1);
                    happySync.accept(2);
                },
                (callback) -> {
                    AsyncRunnable first = (SingleResultCallback<Void> c) -> {
                        happyAsync.accept(1, c);
                    };
                    AsyncRunnable second = (SingleResultCallback<Void> c) -> {
                        happyAsync.accept(2, c);
                    };
                    // This is a simplified variant of the "then" methods;
                    // it has no error handling. It takes methods A and B,
                    // and returns C, which is B(A()).
                    AsyncRunnable combined = (c) -> {
                        first.unsafeFinish((r, e) -> {
                            second.unsafeFinish(c);
                        });
                    };
                    combined.unsafeFinish(callback);
                });

        // This combining method is added as a default method on AsyncRunnable,
        // and a "finish" method wraps the resulting methods. This also adds
        // exception handling and monadic short-circuiting of ensuing methods
        // when an exception arises (comparable to how thrown exceptions "skip"
        // ensuing code).
        assertBehavesSameVariations(3,
                () -> {
                    sync(1);
                    sync(2);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        async(2, c);
                    }).finish(callback);
                });
    }
}
