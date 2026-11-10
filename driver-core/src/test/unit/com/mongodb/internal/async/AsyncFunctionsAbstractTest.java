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
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AsyncFunctionsAbstractTest extends AsyncFunctionsLoopTest {
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

        // 1(plain-1 exception) + 1(sync-2 exception) + 1(success) = 3
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

        // 1(sync-1 exception) + 1(plain-2 exception) + 1(success) = 3
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

        // 1(sync-1 exception) + 1(sync-2 exception) + 1(success) = 3
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

        // 1(plain-11 exception) + 1(sync-1 exception) + 1(plain-22 exception) + 1(sync-2 exception) + 1(success) = 5
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

        // 1(sync-1 exception) + 1(plain-11 exception) + 1(sync-2 exception) + 1(plain-22 exception) + 1(success) = 5
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
                    }).thenRunAndFinish(() -> {
                        plain(22);
                    }, callback);
                });
    }

    @Test
    void testSupply() {
        // 1(sync-0 exception) + 1(plain-1 exception) + 1(syncReturns-2 exception) + 1(success) = 4
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
        // 1(plainTest exception) + 2(true: syncReturns-11 exception/success) + 2(false: plainReturns-22 exception/success) = 5
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
                            // corresponds to a return,
                            // and must be followed by a return or end of method
                            c.complete(plainReturns(22));
                        }
                    }).finish(callback);
                });
    }

    @Test
    void testFullChain() {
        // tests a chain with: runnable, producer, function, function, consumer
        // 13 invocations in sequence, each ending the flow when it throws:
        // 13(one exception each) + 1(all succeed) = 14
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
        // 1(plainTest exception) + 2(true: sync-2 exception/success) + 2(false: sync-3 exception/success) = 5
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
        // 1(sync-0 exception) + 1(plainTest exception)
        // + 4(true: sync-21 exception + sync-22 exception + 2(sync-3 exception/success))
        // + 2(false: sync-3 exception/success) = 8
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
                        }).thenRun(c -> {
                            async(22, c);
                        })
                    ).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });

        // empty `else` branch
        // 1(plainTest exception) + 3(true: syncReturns-2 exception + 2(sync exception/success)) + 1(false: empty branch) = 5
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
                            // do nothing
                            c.complete(c);
                        }
                    }).finish(callback);
                });
    }

    @Test
    void testMixedConditionalCascade() {
        // test1 false: 1(plainTest-2 exception) + 1(test2 true: plain return)
        // + 4(test2 false: syncReturns-33 exception + plain exception + 2(syncReturns-44 exception/success)) = 6
        // 1(plainTest-1 exception) + 2(test1 true: syncReturns-11 exception/success) + 6(test1 false) = 9
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
                            plain(x + 100);
                            asyncReturns(44, c2);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testPlain() {
        // For completeness. This should not be used, since there is no async.
        // 1(plain exception) + 1(plain success) = 2
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
        // 1(sync-1 success) + 2(sync-1 exception: sync-2 exception/success) = 3
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
        // 1(sync-1 success) + 2(sync-1 exception: plain-2 exception/success) = 3
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
        // 2(sync-1 exception: sync-9 exception/success) + 2(sync-2 exception: sync-9 exception/success) + 1(success) = 5
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
        // 1(sync-1 success) + sync-1 exception: 1(sync-8 exception) + 2(sync-9 exception/success) = 4
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
        // try-catch: 1(exceptional: sync-1 exception then sync-2 exception) + 2(normal: sync-1 success, or sync-2 success)
        // 1(exceptional) + 2(normal) * 2(sync-3 exception/success) = 5
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
                    beginAsync().thenRun(c -> {
                        beginAsync().thenRun(c2 -> {
                            async(1, c2);
                        }).onErrorIf(t -> true, (t, c2) -> {
                            async(2, c2);
                        }).finish(c);
                    }).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });

        // multiple catch blocks: Java catch clauses are exclusive, so the handlers
        // share one onErrorIf whose branches mirror the clauses in order. Chained
        // onErrorIf steps would not be exclusive: the IllegalStateException the
        // first handler throws would be offered to the second handler.
        // 1(plainTest exception, matched by neither catch) + 2(true: plain-8 exception/success, then throw)
        // + 2(false: sync-9 exception/success) = 5
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        if (plainTest(1)) {
                            throw new UnsupportedOperationException("A");
                        } else {
                            throw new IllegalStateException("B");
                        }
                    } catch (UnsupportedOperationException e) {
                        plain(8);
                        throw new IllegalStateException("A handled");
                    } catch (IllegalStateException e) {
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
                    }).onErrorIf(t -> true, (t, c) -> {
                        if (t instanceof UnsupportedOperationException) {
                            plain(8);
                            throw new IllegalStateException("A handled");
                        } else {
                            if (t instanceof IllegalStateException) {
                                async(9, c);
                            } else {
                                c.completeExceptionally(t);
                            }
                        }
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithEmptyCatch() {
        // the RuntimeException matches no catch, so only the finally branches: 1(plain-2 exception) + 1(plain-2 success) = 2
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
                            // ignore exceptions
                            c3.complete(c3);
                        }).thenAlwaysRunAndFinish(() -> {
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
        // 1(plain-0 exception) + 1(sync-1 success) + 2(sync-1 exception: plain-2 exception/success, rethrown either way) = 4
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

        // try-catch: 1(plain-0 exception) + 2(sync-1 exception: plain-2 exception/success, rethrown either way) + 1(normal: sync-1 success)
        // 3(exceptional) + 1(normal) * 2(sync-4 exception/success) = 5
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
        // per each of the 2 plainTest values: 2(syncReturns exception -> catch sync-3 exception/success)
        // + 2(sync exception -> catch sync-3 exception/success) + 1(success) = 5
        // 2(plainTest exception -> catch sync-3 exception/success) + 2 * 5 = 12
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
                    beginAsync().<Integer>thenSupply(c -> {
                        int i = plainTest(0) ? 1 : 2;
                        asyncReturns(i + 10, c);
                    }).thenConsume((i, c) -> {
                        async(i + 100, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        async(3, c);
                    }).finish(callback);
                });

        // using an externally-declared variable
        // try-catch, per each of the 2 plainTest values: 2(exceptional: syncReturns or sync threw and catch sync-3 threw)
        // + 3(normal: nothing threw, or either threw and catch sync-3 succeeded)
        // 1(plainTest exception, before the try) + 2 * (2(exceptional) + 3(normal) * 2(trailing sync)) = 17
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
                            i[0] = i2;
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
        // entering the catch costs 2(sync-5 exception, or sync-5 success then rethrow/wrap)
        // per each of the 2 plainTest values: 2(sync-1/2 exception -> catch) + 2(sync-3 exception -> catch) + 1(success) = 5
        // 2(plainTest exception -> catch) + 2 * 5 = 12
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
                                c2.completeExceptionally(t);
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
        // syncReturns-1 exception -> catch: 1(plainTest exception) + 1(true: message differs, rethrow)
        // + 2(false: message matches, syncReturns-2 exception/success) = 4
        // 1(syncReturns-1 success) + 4 = 5
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
        // sync-1 exception -> catch: 1(plainTest exception) + 1(true: message differs, rethrow)
        // + 2(false: message matches, sync-2 exception/success) = 4
        // 1(sync-1 success) + 4 = 5
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
    void testWhile() {
        // last iteration: 3 < 3 = 1
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 1(transition to next iteration) = 4
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 4(transition to next iteration) = 7
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 7(transition to next iteration) = 10
        assertBehavesSameVariations(10,
                () -> {
                    int counter = 0;
                    while (counter < 3 && plainTest(counter)) {
                        counter++;
                        sync(counter);
                    }
                },
                (callback) -> {
                    HoistedLocal<Integer> counter = new HoistedLocal<>(0);
                    beginAsync().thenRunWhileLoop(() -> counter.get() < 3 && plainTest(counter.get()), c -> {
                        counter.set(counter.get() + 1);
                        async(counter.get(), c);
                    }).finish(callback);
                });
    }

    @Test
    void testWhileWithThenRun() {
        // while: last iteration: 3 < 3 = 1
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 1(transition to next iteration) = 4
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 4(transition to next iteration) = 7
        // 1(plainTest exception) + 1(plainTest false) + 1(sync exception) + 1(sync success) * 7(transition to next iteration) = 10
        // trailing sync: 1(exception) + 1(success) = 2
        // 6(while exception) + 4(while success) * 2(trailing sync) = 14
        assertBehavesSameVariations(14,
                () -> {
                    int counter = 0;
                    while (counter < 3 && plainTest(counter)) {
                        counter++;
                        sync(counter);
                    }
                    sync(counter + 1);
                },
                (callback) -> {
                    HoistedLocal<Integer> counter = new HoistedLocal<>(0);
                    beginAsync().thenRunWhileLoop(() -> counter.get() < 3 && plainTest(counter.get()), c -> {
                        counter.set(counter.get() + 1);
                        async(counter.get(), c);
                    }).thenRun(c -> {
                        async(counter.get() + 1, c);
                    }).finish(callback);
                });
    }

    @Test
    void testNestedWhileLoops() {
        // inner while: 4 success + 6 exception = 10
        // last inner iteration: 3 < 3 = 1
        // 1(outer plainTest exception) + 1(outer plainTest false) + (inner while) * 1(transition to next iteration) = 12
        // 1(outer plainTest exception) + 1(outer plainTest false) + (inner while) * 12(transition to next iteration) = 56
        // 1(outer plainTest exception) + 1(outer plainTest false) + (inner while) * 56(transition to next iteration) = 232
        assertBehavesSameVariations(232,
                () -> {
                    int outer = 0;
                    while (outer < 3 && plainTest(outer)) {
                        int inner = 0;
                        while (inner < 3 && plainTest(inner)) {
                            sync(outer + inner);
                            inner++;
                        }
                        outer++;
                    }
                },
                (callback) -> {
                    HoistedLocal<Integer> outer = new HoistedLocal<>(0);
                    beginAsync().thenRunWhileLoop(() -> outer.get() < 3 && plainTest(outer.get()), c -> {
                        HoistedLocal<Integer> inner = new HoistedLocal<>(0);
                        beginAsync().thenRunWhileLoop(
                                () -> inner.get() < 3 && plainTest(inner.get()),
                                c2 -> {
                                    beginAsync().thenRun(c3 -> {
                                        async(outer.get() + inner.get(), c3);
                                    }).thenRun(c3 -> {
                                        inner.set(inner.get() + 1);
                                        c3.complete(c3);
                                    }).finish(c2);
                                }
                        ).thenRun(c2 -> {
                            outer.set(outer.get() + 1);
                            c2.complete(c2);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testWhileLoopStackConstant() {
        int depthWith100 = maxStackDepthForIterations(100);
        int depthWith10000 = maxStackDepthForIterations(10_000);
        assertEquals(depthWith100, depthWith10000, "Stack depth should be constant regardless of iteration count (trampoline)");
    }

    private int maxStackDepthForIterations(final int iterations) {
        HoistedLocal<Integer> counter = new HoistedLocal<>(0);
        HoistedLocal<Integer> maxDepth = new HoistedLocal<>(0);
        beginAsync().thenRunWhileLoop(() -> counter.get() < iterations, c -> {
            maxDepth.set(Math.max(maxDepth.get(), Thread.currentThread().getStackTrace().length));
            counter.set(counter.get() + 1);
            c.complete(c);
        }).finish((v, t) -> {});

        assertEquals(iterations, counter.get());
        return maxDepth.get();
    }

    @Test
    void testRetryLoop() {
        // each iteration consumes 2 options (plainTest + sync), so DEPTH_LIMIT / 2 = 25 iterations fit
        // per iteration: 1(plainTest exception, rethrown) + 1(true: sync-1 success, break) + 1(false: sync-2 success, break)
        // + 1(false: sync-2 exception, rethrown) = 4; only true + sync-1 exception retries
        // 4 * 25(iterations) + 1(forced plainTest exception at the depth limit) = 101 = DEPTH_LIMIT * 2 + 1
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
                            c -> async(plainTest(0) ? 1 : 2, c),
                            e -> e.getMessage().equals("exception-1")
                    ).finish(callback);
                });
    }

    @Test
    void testDoWhileLoop() {
        // each iteration consumes 3 options (plain + sync + plainTest), so 16 whole iterations fit in DEPTH_LIMIT = 50
        // per iteration: 1(plain exception) + 1(sync exception) + 1(plainTest exception) + 1(plainTest false) = 4, true repeats
        // 4 * 16(iterations) + 3(depth-limited 17th: plain exception + sync exception + forced plainTest exception) = 67
        assertBehavesSameVariations(67,
                () -> {
                    do {
                        plain(0);
                        sync(1);
                    } while (plainTest(2));
                },
                (finalCallback) -> {
                    beginAsync().thenRunDoWhileLoop(
                          callback -> {
                              plain(0);
                              async(1, callback);
                          },
                          () -> plainTest(2)
                    ).finish(finalCallback);
                });
    }

    @Test
    void testNestedDoWhileLoops() {
        // inner do-while: 3 success + 5 exception = 8
        // last outer iteration: 3 < 3 = 1
        // 5(inner exception) + 3(inner success) * 1(transition to next iteration) = 8
        // 5(inner exception) + 3(inner success) * (1(outer plainTest exception) + 1(outer plainTest false) + 8(transition to next iteration)) = 35
        // 5(inner exception) + 3(inner success) * (1(outer plainTest exception) + 1(outer plainTest false) + 35(transition to next iteration)) = 116
        assertBehavesSameVariations(116,
                () -> {
                    int outer = 0;
                    do {
                        int inner = 0;
                        do {
                            sync(outer + inner);
                            inner++;
                        } while (inner < 3 && plainTest(inner));
                        outer++;
                    } while (outer < 3 && plainTest(outer));
                },
                (callback) -> {
                    HoistedLocal<Integer> outer = new HoistedLocal<>(0);
                    beginAsync().thenRunDoWhileLoop(c -> {
                        HoistedLocal<Integer> inner = new HoistedLocal<>(0);
                        beginAsync().thenRunDoWhileLoop(c2 -> {
                                    beginAsync().thenRun(c3 -> {
                                        async(outer.get() + inner.get(), c3);
                                    }).thenRun(c3 -> {
                                        inner.set(inner.get() + 1);
                                        c3.complete(c3);
                                    }).finish(c2);
                                }, () -> inner.get() < 3 && plainTest(inner.get())
                        ).thenRun(c2 -> {
                            outer.set(outer.get() + 1);
                            c2.complete(c2);
                        }).finish(c);
                    }, () -> outer.get() < 3 && plainTest(outer.get())).finish(callback);
                });
    }

    @Test
    void testDoWhileLoopStackConstant() {
        int depthWith100 = maxDoWhileStackDepthForIterations(100);
        int depthWith10000 = maxDoWhileStackDepthForIterations(10_000);
        assertEquals(depthWith100, depthWith10000,
                "Stack depth should be constant regardless of iteration count");
    }

    private int maxDoWhileStackDepthForIterations(final int iterations) {
        HoistedLocal<Integer> counter = new HoistedLocal<>(0);
        HoistedLocal<Integer> maxDepth = new HoistedLocal<>(0);
        beginAsync().thenRunDoWhileLoop(c -> {
            maxDepth.set(Math.max(maxDepth.get(), Thread.currentThread().getStackTrace().length));
            counter.set(counter.get() + 1);
            c.complete(c);
        }, () -> counter.get() < iterations).finish((v, t) -> {});
        assertEquals(iterations, counter.get());
        return maxDepth.get();
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
        // 1(plain-1 exception, outside the try)
        // + (in try: sync-2 success + sync-2 exception) * (in finally: plain-3 success + plain-3 exception) = 1 + 2 * 2 = 5
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
    void testSupplyFinallyWithPlainInsideTry() {
        // (in try: normal flow + plain-1 exception + syncReturns-2 exception) * (in finally: normal + exception) = 3 * 2 = 6
        assertBehavesSameVariations(6,
                () -> {
                    try {
                        plain(1);
                        return syncReturns(2);
                    } finally {
                        plain(3);
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        plain(1);
                        asyncReturns(2, c);
                    }).thenAlwaysRunAndFinish(() -> {
                        plain(3);
                    }, callback);
                });
    }

    @Test
    void testSupplyFinallyWithPlainOutsideTry() {
        // 1(plain-1 exception, outside the try)
        // + (in try: syncReturns-2 success + exception) * (in finally: plain-3 success + exception) = 1 + 2 * 2 = 5
        assertBehavesSameVariations(5,
                () -> {
                    plain(1);
                    try {
                        return syncReturns(2);
                    } finally {
                        plain(3);
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        plain(1);
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(2, c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            plain(3);
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testUsedAsLambda() {
        // 1(sync-0 exception) + 1(plain-1 exception) + 1(syncReturns-9 exception) + 1(success) = 4
        assertBehavesSameVariations(4,
                () -> {
                    Supplier<Integer> s = () -> syncReturns(9);
                    sync(0);
                    plain(1);
                    return s.get();
                },
                (callback) -> {
                    AsyncSupplier<Integer> s = c -> asyncReturns(9, c);
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).<Integer>thenSupply(c -> {
                        plain(1);
                        s.getAsync(c);
                    }).finish(callback);
                });
    }

    @Test
    void testVariables() {
        // 1(sync-90 exception) + 1(sync-100 exception) + 1(success) = 3
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
                    }).thenRun(c -> {
                        something[0] = something[0] + 10;
                        async(something[0], c);
                    }).finish(callback);
                });
    }
}
