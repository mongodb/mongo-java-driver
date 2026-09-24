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

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * Covers try/catch/finally and try-with-resources shapes, including nested
 * cleanup try-catch and cross-block value flow through a try. Expected async
 * bodies are what the async generator produces for the corresponding sync body.
 */
abstract class AsyncFunctionsTryCatchTest extends AsyncFunctionsVariableFlowTest {
    @Test
    void testTryCatchFinallyAfterPlainStatements() {
        // plain statements before a try-catch-finally: nested chain with the
        // catch preserved as onErrorIf between the try steps and the finally
        // 1(plainReturns-0 exception) + 1(plain exception), both before the try
        // + (in try: sync success + sync exception) * (in finally: plain success + plain exception) = 2 + 2 * 2 = 6
        assertBehavesSameVariations(6,
                () -> {
                    int client = plainReturns(0);
                    plain(client);
                    try {
                        sync(client);
                    } catch (Exception e) {
                        throw new RuntimeException("wrapped", e);
                    } finally {
                        plain(client);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        int client = plainReturns(0);
                        plain(client);
                        beginAsync().thenRun(c2 -> {
                            async(client, c2);
                        }).onErrorIf(t -> true, (e, c2) -> {
                            throw new RuntimeException("wrapped", e);
                        }).thenAlwaysRunAndFinish(() -> {
                            plain(client);
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithResources() {
        // try-with-resources desugars to declaration + try/finally with close;
        // close never throws (see Resource), so variations = the body's only
        // close() never throws, so only the body branches: 1(sync-1 exception) + 1(sync-1 success) = 2
        assertBehavesSameVariations(2,
                () -> {
                    try (Resource r = new Resource(3)) {
                        sync(1);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        Resource r = new Resource(3);
                        beginAsync().thenRun(c2 -> {
                            async(1, c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            r.close();
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithResourcesWithTrailingPlain() {
        // code after the async call inside the try must go in a following block
        // 1(sync-1 exception) + 1(plain-2 exception) + 1(success) = 3; close() never throws
        assertBehavesSameVariations(3,
                () -> {
                    try (Resource r = new Resource(3)) {
                        sync(1);
                        plain(2);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        Resource r = new Resource(3);
                        beginAsync().thenRun(c2 -> {
                            async(1, c2);
                        }).thenRun(c2 -> {
                            plain(2);
                            c2.complete(c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            r.close();
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyTryWithResources() {
        // 1(syncReturns-1 exception) + 1(syncReturns-1 success) = 2; close() never throws
        assertBehavesSameVariations(2,
                () -> {
                    try (Resource r = new Resource(3)) {
                        return syncReturns(1);
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        Resource r = new Resource(3);
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(1, c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            r.close();
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithResourcesWithNestedTryCatch() {
        // a try/catch nested inside try-with-resources must be rendered as its
        // own chain step (statement loss here is the worst failure class)
        // 1(plain-1 exception) + 1(sync-2 success) + 2(sync-2 exception: plain-3 exception/success, rethrown either way) = 4
        assertBehavesSameVariations(4,
                () -> {
                    try (Resource r = new Resource(3)) {
                        plain(1);
                        try {
                            sync(2);
                        } catch (Exception e) {
                            plain(3);
                            throw e;
                        }
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        Resource r = new Resource(3);
                        beginAsync().thenRun(c2 -> {
                            plain(1);
                            c2.complete(c2);
                        }).thenRunTryCatchAsyncBlocks(c2 -> {
                            async(2, c2);
                        }, Exception.class, (e, c2) -> {
                            plain(3);
                            c2.completeExceptionally(e);
                        }).thenAlwaysRunAndFinish(() -> {
                            r.close();
                        }, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWithResourcesWithCrossBlockVariable() {
        // a variable declared before the try-with-resources, assigned inside it,
        // and read after it: bare declaration is replaced by the array wrap
        // (no shadow decl), and reads after the try use var[0], including in
        // if-conditions
        // 1(sync-1 exception) + 1(plainReturns-2 exception) + 1(plain-3 exception) + 2(sync-4 exception/success) = 5
        assertBehavesSameVariations(5,
                () -> {
                    int x;
                    try (Resource r = new Resource(3)) {
                        sync(1);
                        x = plainReturns(2);
                    }
                    if (x != 0) {
                        plain(3);
                    }
                    sync(4);
                },
                (callback) -> {
                    final int[] x = new int[1];
                    beginAsync().thenRun(c -> {
                        Resource r = new Resource(3);
                        beginAsync().thenRun(c2 -> {
                            async(1, c2);
                        }).thenRun(c2 -> {
                            x[0] = plainReturns(2);
                            c2.complete(c2);
                        }).thenAlwaysRunAndFinish(() -> {
                            r.close();
                        }, c);
                    }).thenRun(c -> {
                        if (x[0] != 0) {
                            plain(3);
                        }
                        async(4, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryCatchResultReadAfterTry() {
        // The try's produced value is only read AFTER the try: the nested chain
        // must write it back into the array-wrapped variable via thenConsume
        // (dropping it into a Void finish would silently yield null)
        // 1(sync-1 exception) + 1(syncReturns-2 exception, wrapped) + 2(trailing sync exception/success) = 4
        assertBehavesSameVariations(4,
                () -> {
                    sync(1);
                    int x;
                    try {
                        x = syncReturns(2);
                    } catch (Throwable t) {
                        throw new IllegalStateException("wrapped");
                    }
                    sync(x + 10);
                },
                (callback) -> {
                    final int[] x = new int[1];
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(2, c2);
                        }).thenConsume((x2, c2) -> {
                            x[0] = x2;
                            c2.complete(c2);
                        }).onErrorIf(t -> true, (t, c2) -> {
                            throw new IllegalStateException("wrapped");
                        }).finish(c);
                    }).thenRun(c -> {
                        async(x[0] + 10, c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplierInTryCatchGuardRethrow() {
        // Supplier local used inside try; catch is guard-style (no else):
        // if (cond) { return ...; } throw e;
        // syncReturns-9 exception -> catch: 1(plainTest exception) + 2(true: syncReturns-2 exception/success) + 1(false: rethrow) = 4
        // 1(syncReturns-9 success) + 4 = 5
        assertBehavesSameVariations(5,
                () -> {
                    Supplier<Integer> s = () -> syncReturns(9);
                    try {
                        return s.get();
                    } catch (Exception e) {
                        if (plainTest(1)) {
                            return syncReturns(2);
                        }
                        throw e;
                    }
                },
                (callback) -> {
                    AsyncSupplier<Integer> s = c -> asyncReturns(9, c);
                    beginAsync().<Integer>thenSupply(c -> {
                        s.getAsync(c);
                    }).onErrorIf(e -> plainTest(1), (t, c) -> {
                        asyncReturns(2, c);
                    }).finish(callback);
                });
    }

    @Test
    void testTryMultipleAsyncCatchRethrowOrWrap() {
        // Multiple async calls in try; catch tests, rethrows or wraps
        // entering the catch costs 1(plain-0 exception) + 1(plainTest exception) + 1(true: rethrow) + 1(false: wrap) = 4
        // 4(sync-1 exception -> catch) + 4(sync-2 exception -> catch) + 1(success) = 9
        assertBehavesSameVariations(9,
                () -> {
                    try {
                        sync(1);
                        sync(2);
                    } catch (Exception e) {
                        plain(0);
                        if (plainTest(1)) {
                            throw e;
                        }
                        throw new RuntimeException("wrapped", e);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        async(2, c);
                    }).onErrorIf(t -> true, (e, c) -> {
                        plain(0);
                        if (plainTest(1)) {
                            c.completeExceptionally(e);
                            return;
                        }
                        throw new RuntimeException("wrapped", e);
                    }).finish(callback);
                });
    }

    @Test
    void testTryPlainStatementThenAsyncWithTypedCatchWrap() {
        // A plain statement and the async call share one chain step inside the try, and
        // the typed catch renders as the onErrorIf right after that step. A throw from
        // the step already routes to that onErrorIf, so the catch must NOT also be
        // restated as an inline try/catch around the plain statement.
        // 1(plainReturns-1 exception, wrapped) + 1(sync-x success) + 1(sync-x exception, wrapped) = 3
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        int x = plainReturns(1);
                        sync(x);
                    } catch (RuntimeException e) {
                        throw new IllegalStateException("wrapped", e);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        int x = plainReturns(1);
                        async(x, c);
                    }).onErrorIf(t -> t instanceof RuntimeException, (e, c) -> {
                        throw new IllegalStateException("wrapped", e);
                    }).finish(callback);
                });
    }

    @Test
    void testTryWholeBodyValueFlowWithCrossBlockLocal() {
        // Whole-body try: value flows SUPPLY -> APPLY -> APPLY, an intermediate local
        // crosses blocks (array-wrapped), catch translates everything
        // the catch wraps and rethrows without branching, so each of the 4 invocations ends the flow when it throws
        // 4(one exception each) + 1(all succeed) = 5
        assertBehavesSameVariations(5,
                () -> {
                    try {
                        int len = syncReturns(1);
                        int size = plainReturns(len);
                        int data = syncReturns(size);
                        return plainReturns(data + size);
                    } catch (Throwable t) {
                        throw new RuntimeException("translated", t);
                    }
                },
                (callback) -> {
                    final int[] size = new int[1];
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).<Integer>thenApply((len, c) -> {
                        size[0] = plainReturns(assertNotNull(len));
                        asyncReturns(size[0], c);
                    }).<Integer>thenApply((data, c) -> {
                        c.complete(plainReturns(assertNotNull(data) + size[0]));
                    }).onErrorIf(t -> true, (t, c) -> {
                        throw new RuntimeException("translated", t);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyTryCatchRethrow() {
        // Value-producing try with a rethrowing catch: typed thenSupply + onErrorIf
        // (no supplier-typed thenRunTryCatchAsyncBlocks exists)
        // 1(syncReturns-1 success) + 2(syncReturns-1 exception: plain-0 exception/success, rethrown either way) = 3
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        return syncReturns(1);
                    } catch (Throwable t) {
                        plain(0);
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        plain(0);
                        c.completeExceptionally(t);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyTryCatchRethrowAfterBlock() {
        // Value-producing try-catch-rethrow preceded by an async block: the
        // try-catch nests (so onErrorIf can't swallow pre-try errors) and the
        // value flows out through the wrapping thenSupply
        // 1(sync-1 exception) + 1(syncReturns-2 success) + 2(syncReturns-2 exception: plain-3 exception/success, rethrown either way) = 4
        assertBehavesSameVariations(4,
                () -> {
                    sync(1);
                    try {
                        return syncReturns(2);
                    } catch (Throwable t) {
                        plain(3);
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).<Integer>thenSupply(c -> {
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(2, c2);
                        }).onErrorIf(t -> true, (t, c2) -> {
                            plain(3);
                            c2.completeExceptionally(t);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyTryCatchRethrowAfterPlainStatements() {
        // plain statements before a value-producing try-catch-rethrow merge into
        // the wrapping thenSupply lambda (locals stay lexically capturable — no
        // array wrap); pre-try errors bypass the onErrorIf via the outer lambda
        // 1(plainReturns-1 exception) + 1(plain-x exception), both before the try
        // + 1(syncReturns-x success) + 2(syncReturns-x exception: plain-x exception/success, rethrown either way) = 5
        assertBehavesSameVariations(5,
                () -> {
                    int x = plainReturns(1);
                    plain(x);
                    try {
                        return syncReturns(x);
                    } catch (Throwable t) {
                        plain(x);
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        int x = plainReturns(1);
                        plain(x);
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(x, c2);
                        }).onErrorIf(t -> true, (t, c2) -> {
                            plain(x);
                            c2.completeExceptionally(t);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testCatchWithNestedCleanupTryCatch() {
        // Catch body does multi-statement cleanup in a nested try/catch
        // (suppressing cleanup failures), then rethrows
        // 1(sync-1 success) + 2(sync-1 exception: cleanup plain-2 exception/success, t rethrown either way) = 3
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        sync(1);
                    } catch (Throwable t) {
                        try {
                            plain(2);
                        } catch (RuntimeException suppressed) {
                            t.addSuppressed(suppressed);
                        }
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().thenRunTryCatchAsyncBlocks(c -> {
                        async(1, c);
                    }, Throwable.class, (t, c) -> {
                        try {
                            plain(2);
                        } catch (RuntimeException suppressed) {
                            t.addSuppressed(suppressed);
                        }
                        c.completeExceptionally(t);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyCatchWithNestedCleanupTryCatch() {
        // Value-producing try whose catch does nested-try cleanup then rethrows
        // 1(syncReturns-1 success) + 2(syncReturns-1 exception: cleanup plain-2 exception/success, t rethrown either way) = 3
        assertBehavesSameVariations(3,
                () -> {
                    try {
                        return syncReturns(1);
                    } catch (Throwable t) {
                        try {
                            plain(2);
                        } catch (RuntimeException suppressed) {
                            t.addSuppressed(suppressed);
                        }
                        throw t;
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        try {
                            plain(2);
                        } catch (RuntimeException suppressed) {
                            t.addSuppressed(suppressed);
                        }
                        c.completeExceptionally(t);
                    }).finish(callback);
                });
    }

    @Test
    void testSupplyFinallyWithValueFlowAndConditionInFinally() {
        // Value flows SUPPLY -> APPLY inside try; finally contains a conditional
        // in try: 1(syncReturns-1 exception) + 1(plainReturns exception) + 1(normal) = 3
        // in finally: 1(plainTest exception) + 2(true: plain-0 exception/success) + 1(false) = 4
        // 3 * 4 = 12
        assertBehavesSameVariations(12,
                () -> {
                    try {
                        int i = syncReturns(1);
                        return plainReturns(i);
                    } finally {
                        if (plainTest(1)) {
                            plain(0);
                        }
                    }
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).<Integer>thenApply((i, c) -> {
                        c.complete(plainReturns(assertNotNull(i)));
                    }).thenAlwaysRunAndFinish(() -> {
                        if (plainTest(1)) {
                            plain(0);
                        }
                    }, callback);
                });
    }

    @Test
    void testTryPlainBetweenAsyncCalls() {
        // A plain statement sits between two async calls inside the try; it opens
        // the second step. The catch body appears once, in the onErrorIf, not inline.
        // 2(sync-1 exception: plain-0 exception/success) + 2(plainReturns-2
        // exception: plain-0 exception/success) + 2(sync-x exception: plain-0
        // exception/success) + 1(all succeed) = 7
        assertBehavesSameVariations(7,
                () -> {
                    try {
                        sync(1);
                        int x = plainReturns(2);
                        sync(x);
                    } catch (Throwable t) {
                        plain(0);
                        throw new RuntimeException("translated", t);
                    }
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRun(c -> {
                        int x = plainReturns(2);
                        async(x, c);
                    }).onErrorIf(t -> true, (t, c) -> {
                        plain(0);
                        throw new RuntimeException("translated", t);
                    }).finish(callback);
                });
    }
}
