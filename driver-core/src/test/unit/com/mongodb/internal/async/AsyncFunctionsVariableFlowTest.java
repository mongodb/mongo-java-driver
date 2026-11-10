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

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * Covers variable-flow shapes: values produced by async calls flowing through
 * supply/apply chains and conditionals over such values.
 * Expected async bodies are what the async generator produces for the
 * corresponding sync body.
 */
abstract class AsyncFunctionsVariableFlowTest extends AsyncFunctionsTestBase {
    @Test
    void testSupplyDirectDelegation() {
        // a body that is nothing but a value-returning delegation still wraps
        // in a chain: finish() routes synchronous throws to the callback, so
        // generated async methods never throw synchronously
        // 1(syncReturns-1 exception) + 1(syncReturns-1 success) = 2
        assertBehavesSameVariations(2,
                () -> {
                    return syncReturns(1);
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).finish(callback);
                });
    }

    @Test
    void testIfElseWithAsyncInBothBranchesAndTail() {
        // 1(plainTest exception)
        // + true path: 1(sync exception) + 2(tail plain exception/success)
        // + false path: 1(plain exception) + 1(sync exception) + 2(tail plain exception/success)
        assertBehavesSameVariations(8,
                () -> {
                    if (plainTest(1)) {
                        sync(2);
                    } else {
                        plain(3);
                        sync(4);
                    }
                    plain(5);
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        if (plainTest(1)) {
                            async(2, c);
                        } else {
                            plain(3);
                            async(4, c);
                        }
                    }).thenRun(c -> {
                        plain(5);
                        c.complete(c);
                    }).finish(callback);
                });
    }

    @Test
    void testConditionalInitFromAsyncAssignment() {
        // conditional init: an if-body assigning an outer variable from an async
        // call gets a thenRunIf whose chain writes the value back (CONSUME)
        // 1(plainTest exception)
        // + 4(true: v is null, so syncReturns-1 exception + plain-2 exception + 2(trailing plain exception/success))
        // + 3(false: plainReturns-5 exception + 2(trailing plain exception/success)) = 8
        assertBehavesSameVariations(8,
                () -> {
                    Integer v = plainTest(9) ? null : plainReturns(5);
                    if (v == null) {
                        v = syncReturns(1);
                        plain(2);
                    }
                    plain(v + 100);
                },
                (callback) -> {
                    final Integer[] v = new Integer[1];
                    beginAsync().thenRun(c -> {
                        v[0] = plainTest(9) ? null : plainReturns(5);
                        c.complete(c);
                    }).thenRunIf(() -> v[0] == null,
                        beginAsync().<Integer>thenSupply(c -> {
                            asyncReturns(1, c);
                        }).thenConsume((v2, c) -> {
                            v[0] = v2;
                            plain(2);
                            c.complete(c);
                    })).thenRunAndFinish(() -> {
                            plain(v[0] + 100);
                        }, callback);
                });
    }

    @Test
    void testVariableFromAsyncCall() {
        // Variable declared and assigned from async call result, then used
        // Should flow through SUPPLY→APPLY, not cross-block array
        // 1(syncReturns-10 exception) + 1(plain exception) + 1(success) = 3
        assertBehavesSameVariations(3,
                () -> {
                    int result = syncReturns(10);
                    plain(result);
                    return result;
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(10, c);
                    }).<Integer>thenApply((result, c) -> {
                        plain(assertNotNull(result));
                        c.complete(result);
                    }).finish(callback);
                });
    }

    @Test
    void testVariableFromAsyncCallWithCondition() {
        // Variable from async call used in condition
        // 1(syncReturns-10 exception) + 1(syncReturns-10 success; the condition itself does not branch) = 2
        assertBehavesSameVariations(2,
                () -> {
                    int result = syncReturns(10);
                    if (result < 0) {
                        throw new RuntimeException("Negative result");
                    }
                    return result;
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(10, c);
                    }).<Integer>thenApply((result, c) -> {
                        if (assertNotNull(result) < 0) {
                            throw new RuntimeException("Negative result");
                        }
                        c.complete(result);
                    }).finish(callback);
                });
    }

    @Test
    void testReturnWrappedAsyncResult() {
        // A plain wrapper around the async call in a return statement:
        // the value flows SUPPLY -> APPLY, wrapper applied on completion
        // 1(syncReturns-1 exception) + 1(plainReturns exception) + 1(success) = 3
        assertBehavesSameVariations(3,
                () -> {
                    return plainReturns(syncReturns(1));
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).<Integer>thenApply((r, c) -> {
                        c.complete(plainReturns(assertNotNull(r)));
                    }).finish(callback);
                });
    }

    @Test
    void testConditionReadingCrossBlockVariable() {
        // A cross-block (array-wrapped) variable read in a thenRunIf condition
        // must be dereferenced there too: b -> b[0]
        // 1(plainTest exception)
        // + 4(true: sync-1 exception + sync-2 exception + 2(sync-3 exception/success))
        // + 3(false: sync-1 exception + 2(sync-3 exception/success)) = 8
        assertBehavesSameVariations(8,
                () -> {
                    boolean b = plainTest(1);
                    sync(1);
                    if (b) {
                        sync(2);
                    }
                    sync(3);
                },
                (callback) -> {
                    final boolean[] b = new boolean[1];
                    beginAsync().thenRun(c -> {
                        b[0] = plainTest(1);
                        async(1, c);
                    }).thenRunIf(() -> b[0], c -> {
                        async(2, c);
                    }).thenRun(c -> {
                        async(3, c);
                    }).finish(callback);
                });
    }

    @Test
    void testVariableReassignedFromAsyncCall() {
        // Variable assigned from an async call, then reassigned from another:
        // value flows through the APPLY chain, no container needed
        // 1(syncReturns-1 exception) + 1(plain exception) + 1(syncReturns-(x+10) exception) + 1(plain exception)
        // + 1(success) = 5
        assertBehavesSameVariations(5,
                () -> {
                    int x = syncReturns(1);
                    plain(x);
                    x = syncReturns(x + 10);
                    plain(x);
                },
                (callback) -> {
                    beginAsync().<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).<Integer>thenApply((x, c) -> {
                        plain(assertNotNull(x));
                        asyncReturns(x + 10, c);
                    }).thenConsume((x, c) -> {
                        plain(assertNotNull(x));
                        c.complete(c);
                    }).finish(callback);
                });
    }

    @Test
    void testVariableReassignedPlainAcrossBlocks() {
        // Plain local declared with initializer, reassigned in a later block
        // 1(plainReturns-1 exception) + 1(sync-i exception) + 1(sync-(i+10) exception) + 1(success) = 4
        assertBehavesSameVariations(4,
                () -> {
                    int i = plainReturns(1);
                    sync(i);
                    i = i + 10;
                    sync(i);
                },
                (callback) -> {
                    final int[] i = new int[1];
                    beginAsync().thenRun(c -> {
                        i[0] = plainReturns(1);
                        async(i[0], c);
                    }).thenRun(c -> {
                        i[0] = i[0] + 10;
                        async(i[0], c);
                    }).finish(callback);
                });
    }

    @Test
    void testReturnDeeplyNestedAsyncResult() {
        // Async call nested inside a larger returned expression: desugars to
        // decl-from-async-call + return, flowing SUPPLY -> APPLY
        // 1(sync-0 exception) + 1(syncReturns-1 exception) + 1(inner plainReturns exception) + 1(outer plainReturns exception)
        // + 1(success) = 5
        assertBehavesSameVariations(5,
                () -> {
                    sync(0);
                    return plainReturns(plainReturns(syncReturns(1)));
                },
                (callback) -> {
                    beginAsync().thenRun(c -> {
                        async(0, c);
                    }).<Integer>thenSupply(c -> {
                        asyncReturns(1, c);
                    }).<Integer>thenApply((result, c) -> {
                        c.complete(plainReturns(plainReturns(assertNotNull(result))));
                    }).finish(callback);
                });
    }

    @Test
    void testGuardReassignedVariableThenReassignedFromAsync() {
        // A step-local declared+initialized in the first block, null-tested in a
        // thenRunIf guard whose body reassigns it from an async call, then
        // reassigned again from a second async call whose result is read by a
        // following plain statement. The last assignment rides the chain, but the
        // earlier write/read pairs still cross lambda boundaries, so the variable
        // must be wrapped.
        // 1(plainTest exception)
        // + 5(true: syncReturns-1 exception + plain-2 exception + 3(trailing syncReturns exception + plain exception/success))
        // + 4(false: plainReturns-5 exception + 3(trailing syncReturns exception + plain exception/success)) = 10
        assertBehavesSameVariations(10,
                () -> {
                    Integer v = plainTest(9) ? null : plainReturns(5);
                    if (v == null) {
                        v = syncReturns(1);
                        plain(2);
                    }
                    v = syncReturns(v + 10);
                    plain(v + 100);
                },
                (callback) -> {
                    final Integer[] v = new Integer[1];
                    beginAsync().thenRun(c -> {
                        v[0] = plainTest(9) ? null : plainReturns(5);
                        c.complete(c);
                    }).thenRunIf(() -> v[0] == null,
                        beginAsync().<Integer>thenSupply(c -> {
                            asyncReturns(1, c);
                        }).thenConsume((v2, c) -> {
                            v[0] = v2;
                            plain(2);
                            c.complete(c);
                        })
                    ).<Integer>thenSupply(c -> {
                        asyncReturns(v[0] + 10, c);
                    }).thenConsume((v2, c) -> {
                        v[0] = v2;
                        plain(v2 + 100);
                        c.complete(c);
                    }).finish(callback);
                });
    }
}
