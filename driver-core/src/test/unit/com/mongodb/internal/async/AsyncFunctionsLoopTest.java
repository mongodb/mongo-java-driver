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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * Covers while/retry-loop shapes: loop-carried variables, try-catch bodies inside
 * loops, and reassignment-from-async within loop conditions. Expected async bodies
 * are what the async generator produces for the corresponding sync body.
 */
abstract class AsyncFunctionsLoopTest extends AsyncFunctionsTryCatchTest {
    @Test
    void testWhileLoopWithGenericCrossBlockVariable() {
        // a generic-typed local declared in a leading block and consumed by the
        // loop cannot be array-wrapped (generic array creation); it is hoisted
        // as a null-initialized HoistedLocal instead
        // 1(success) + 1(plain exception) + 2(sync exception per iteration) = 4
        assertBehavesSameVariations(4,
                () -> {
                    plain(0);
                    Deque<Integer> queue = new ArrayDeque<>(Arrays.asList(1, 2));
                    while (!queue.isEmpty()) {
                        sync(queue.poll());
                    }
                },
                (callback) -> {
                    HoistedLocal<Deque<Integer>> queue = HoistedLocal.unassigned();
                    beginAsync().thenRun(c -> {
                        plain(0);
                        queue.set(new ArrayDeque<>(Arrays.asList(1, 2)));
                        c.complete(c);
                    }).thenRunWhileLoop(() -> !queue.get().isEmpty(), c -> {
                        async(queue.get().poll(), c);
                    }).finish(callback);
                });
    }

    @Test
    void testWhileLoopWithTryCatchBody() {
        // per-iteration catch: the loop continues after a swallowed exception
        // 1(success) + per iteration [1(plain exception in catch) + 1(caught-and-continued sync exception) + 1(sync success)] = 7
        assertBehavesSameVariations(7,
                () -> {
                    Deque<Integer> queue = new ArrayDeque<>(Arrays.asList(1, 2));
                    while (!queue.isEmpty()) {
                        try {
                            sync(queue.poll());
                        } catch (RuntimeException e) {
                            plain(0);
                        }
                    }
                },
                (callback) -> {
                    HoistedLocal<Deque<Integer>> queue = new HoistedLocal<>(new ArrayDeque<>(Arrays.asList(1, 2)));
                    beginAsync().thenRunWhileLoop(() -> !queue.get().isEmpty(), c -> {
                        beginAsync().thenRunTryCatchAsyncBlocks(c2 -> {
                            async(queue.get().poll(), c2);
                        }, RuntimeException.class, (e, c2) -> {
                            plain(0);
                            c2.complete(c2);
                        }).finish(c);
                    }).finish(callback);
                });
    }

    @Test
    void testRetryLoopBreakInsideTry() {
        // Retry-loop variant with the loop exit at the end of the try body
        // (break-inside-try) instead of after the try-catch
        // each iteration consumes 2 options (plainTest + sync), so DEPTH_LIMIT / 2 = 25 iterations fit
        // per iteration: 1(plainTest exception, rethrown) + 1(true: sync-1 success, break) + 1(false: sync-2 success, break)
        // + 1(false: sync-2 exception, rethrown) = 4; only true + sync-1 exception retries
        // 4 * 25(iterations) + 1(forced plainTest exception at the depth limit) = 101 = DEPTH_LIMIT * 2 + 1
        assertBehavesSameVariations(InvocationTracker.DEPTH_LIMIT * 2 + 1,
                () -> {
                    while (true) {
                        try {
                            sync(plainTest(0) ? 1 : 2);
                            break;
                        } catch (RuntimeException e) {
                            if (e.getMessage().equals("exception-1")) {
                                continue;
                            }
                            throw e;
                        }
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
    void testWhileLoopReassignedFromAsync() {
        // loop-carried variable reassigned from an async call inside the loop
        // body, feeding the loop condition; a writeback thenConsume carries the
        // value back into the HoistedLocal before the next condition check
        // each iteration consumes 3 options (plainTest + syncReturns + plain), so 17 iterations are entered before DEPTH_LIMIT = 50
        // per iteration: 1(plainTest exception) + 2(plainTest false: trailing plain exception/success) + 1(syncReturns exception)
        // + 1(plain exception) = 5; plain success transitions to the next iteration
        // the 17th iteration contributes the same 5, its plain being forced to throw at the depth limit
        // 5 * 17(iterations) = 85
        assertBehavesSameVariations(85,
                () -> {
                    int v = 1;
                    while (plainTest(v)) {
                        v = syncReturns(v + 1);
                        plain(v);
                    }
                    plain(v + 100);
                },
                (callback) -> {
                    HoistedLocal<Integer> v = new HoistedLocal<>(1);
                    beginAsync().thenRunWhileLoop(() -> plainTest(v.get()), c -> {
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(v.get() + 1, c2);
                        }).thenConsume((v2, c2) -> {
                            v.set(v2);
                            plain(v.get());
                            c2.complete(c2);
                        }).finish(c);
                    }).thenRunAndFinish(() -> {
                        plain(v.get() + 100);
                    }, callback);
                });
    }

    @Test
    void testTryCatchWithWhileLoopReassignment() {
        // whole-body try with an always-throwing catch around a reassignment
        // loop: flat chain + onErrorIf; the HoistedLocal declaration is hoisted
        // above beginAsync() with its initializer kept inside the try
        // each iteration consumes 2 options (plainTest + syncReturns), so DEPTH_LIMIT / 2 = 25 iterations fit
        // per iteration: 1(plainTest exception, wrapped) + 2(plainTest false: trailing plain exception/success)
        // + 1(syncReturns exception, wrapped) = 4; syncReturns success transitions to the next iteration
        // 4 * 25(iterations) + 1(forced plainTest exception at the depth limit, wrapped) = 101
        assertBehavesSameVariations(101,
                () -> {
                    try {
                        int v = 1;
                        while (plainTest(v)) {
                            v = syncReturns(v + 1);
                        }
                        plain(v);
                    } catch (Exception e) {
                        throw new RuntimeException("wrapped", e);
                    }
                },
                (callback) -> {
                    HoistedLocal<Integer> v = HoistedLocal.unassigned();
                    beginAsync().thenRun(c -> {
                        v.set(1);
                        c.complete(c);
                    }).thenRunWhileLoop(() -> plainTest(v.get()), c -> {
                        beginAsync().<Integer>thenSupply(c2 -> {
                            asyncReturns(v.get() + 1, c2);
                        }).thenConsume((v2, c2) -> {
                            v.set(v2);
                            c2.complete(c2);
                        }).finish(c);
                    }).thenRun(c -> {
                        plain(v.get());
                        c.complete(c);
                    }).onErrorIf(t -> true, (e, c) -> {
                        throw new RuntimeException("wrapped", e);
                    }).finish(callback);
                });
    }

    @Test
    void testWhileLoopNullCheckOnLoopCarriedVar() {
        // A loop-carried variable checked against null in the loop condition must
        // read via getNullable() — HoistedLocal.get() asserts non-null.
        // the body nulls the variable, so the loop runs exactly once
        // 1(sync exception) + 2(trailing plain exception/success) = 3
        assertBehavesSameVariations(3,
                () -> {
                    Integer v = 1;
                    while (v != null) {
                        sync(v);
                        v = null;
                    }
                    plain(200);
                },
                (callback) -> {
                    HoistedLocal<Integer> v = new HoistedLocal<>(1);
                    beginAsync().thenRunWhileLoop(() -> v.getNullable() != null, c -> {
                        beginAsync().thenRun(c2 -> {
                            async(v.get(), c2);
                        }).thenRun(c2 -> {
                            v.set(null);
                            c2.complete(c2);
                        }).finish(c);
                    }).thenRunAndFinish(() -> {
                        plain(200);
                    }, callback);
                });
    }
}
