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

import com.mongodb.client.TestListener;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class AsyncFunctionsTest {
    private final TestListener listener = new TestListener();
    private final InvocationTracker invocationTracker = new InvocationTracker();
    private boolean throwExceptionsFromAsync = false;

    @Test
    void testBasicVariations1() {
        /*
        Some of our methods have "Async" counterparts. These "Async" methods
        must implement the same behaviour asynchronously. In these "Async"
        methods, a SingleResultCallback is provided as a parameter, and the
        method calls at least one other "Async" method (or it invokes a
        non-driver async API).

        The API tested here facilitates the writing of such methods using
        standardized, tested, and non-nested boilerplate. For example, given
        the following "sync" method:

            public T myMethod()
                sync(1);
            }

        The async counterpart would be:

            public void myMethodAsync(SingleResultCallback<T> callback)
                beginAsync().thenRun(c -> { // 1, 2
                    async(1, c);            // 3, 4
                }).finish(callback);        // 5
            }

        Usage:
        1. Start an async chain using the "beginAsync" static method.
        2. Use an appropriate chaining method (then...), which will provide "c"
        3. copy all sync code into that method; convert sync methods to async
        4. at any async method, pass in "c", and end that "block"
        5. provide the original "callback" at the end of the chain via "finish"

        (The above example is tested at the end of this method, and other tests
        will provide additional examples.)

        Requirements and conventions:

        Each async lambda MUST invoke its async method with "c", and MUST return
        immediately after invoking that method. It MUST NOT, for example, have
        a catch or finally (including close on try-with-resources) after the
        invocation of the async method.

        In cases where the async method has "mixed" returns (some of which are
        plain sync, some async), the "c" callback MUST be completed on the
        plain sync path, `c.complete()`, followed by a return or end of method.

        Chains starting with "beginAsync" correspond roughly to code blocks.
        This includes the method body, blocks used in if/try/catch/while/etc.
        statements, and places where anonymous code blocks might be used. For
        clarity, such nested/indented chains might be omitted (where possible,
        as demonstrated in the tests/examples below).

        Plain sync code MAY throw exceptions, and SHOULD NOT attempt to handle
        them asynchronously. The exceptions will be caught and handled by the
        code blocks that contain this sync code.

        All code, including "plain" code (parameter checks) SHOULD be placed
        within the "boilerplate". This ensures that exceptions are handled,
        and facilitates comparison/review. This excludes code that must be
        "shared", such as lambda and variable declarations.

        A curly-braced lambda body (with no linebreak before "."), as shown
        below, SHOULD be used (for consistency, and ease of comparison/review).
        */

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
        /*
        Code review checklist for async code:

        1. Is everything inside the boilerplate?
        2. Is "callback" supplied to "finish"?
        3. In each block and nested block, is that same block's "c" always passed/completed at the end of execution?
        4. Is every c.complete followed by a return, to end execution?
        5. Have all sync method calls been converted to async, where needed?
        */
    }

    @Test
    void testBasicVariations2() {
        // tests pairs
        // converting: plain-sync, sync-plain, sync-sync
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
    void testBasicVariations4() {
        // tests the sync-sync pair with preceding and ensuing plain methods:
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
    void testSupplyMixed() {
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

    @SuppressWarnings("ConstantConditions")
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
    void testConditionalVariations() {
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
        // for completeness; should not be used, since there is no async
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

        // chain of 2 in try
        // "onErrorIf" will consider everything in
        // the preceding chain to be part of the try
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
    void testLoop() {
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
                            c -> sync(plainTest(0) ? 1 : 2),
                            e -> e.getMessage().equals("exception-1")
                    ).finish(callback);
                });
    }

    @Test
    void testFinally() {
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
    void testInvalid() {
        assertThrows(IllegalStateException.class, () -> {
            beginAsync().thenRun(c -> {
                async(3, c);
                throw new IllegalStateException("must not cause second callback invocation");
            }).finish((v, e) -> {});
        });
        assertThrows(IllegalStateException.class, () -> {
            beginAsync().thenRun(c -> {
                async(3, c);
            }).finish((v, e) -> {
                throw new IllegalStateException("must not cause second callback invocation");
            });
        });
    }

    @Test
    void testDerivation() {
        // Demonstrates the progression from nested async to the API.

        // Stand-ins for sync-async methods; these "happily" do not throw
        // exceptions, to avoid complicating this demo async code.
        Consumer<Integer> happySync = (i) -> {
            invocationTracker.getNextOption(1);
            listener.add("affected-success-" + i);
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

    // invoked methods:

    private void plain(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("plain-exception-" + i);
            throw new RuntimeException("affected method exception-" + i);
        } else {
            listener.add("plain-success-" + i);
        }
    }

    private int plainReturns(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("plain-exception-" + i);
            throw new RuntimeException("affected method exception-" + i);
        } else {
            listener.add("plain-success-" + i);
            return i;
        }
    }

    private boolean plainTest(final int i) {
        int cur = invocationTracker.getNextOption(3);
        if (cur == 0) {
            listener.add("plain-exception-" + i);
            throw new RuntimeException("affected method exception-" + i);
        } else if (cur == 1) {
            listener.add("plain-false-" + i);
            return false;
        } else {
            listener.add("plain-true-" + i);
            return true;
        }
    }

    private void sync(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("affected-exception-" + i);
            throw new RuntimeException("exception-" + i);
        } else {
            listener.add("affected-success-" + i);
        }
    }

    private Integer syncReturns(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("affected-exception-" + i);
            throw new RuntimeException("exception-" + i);
        } else {
            listener.add("affected-success-" + i);
            return i;
        }
    }

    private void async(final int i, final SingleResultCallback<Void> callback) {
        if (throwExceptionsFromAsync) {
            sync(i);
            callback.complete(callback);

        } else {
            try {
                sync(i);
                callback.complete(callback);
            } catch (Throwable t) {
                callback.onResult(null, t);
            }
        }
    }

    private void asyncReturns(final int i, final SingleResultCallback<Integer> callback) {
        if (throwExceptionsFromAsync) {
            callback.complete(syncReturns(i));
        } else {
            try {
                callback.complete(syncReturns(i));
            } catch (Throwable t) {
                callback.onResult(null, t);
            }
        }
    }

    // assert methods:

    private void assertBehavesSameVariations(final int expectedVariations, final Runnable sync,
            final Consumer<SingleResultCallback<Void>> async) {
        assertBehavesSameVariations(expectedVariations,
                () -> {
                    sync.run();
                    return null;
                },
                (c) -> {
                    async.accept((v, e) -> c.onResult(v, e));
                });
    }

    private <T> void assertBehavesSameVariations(final int expectedVariations, final Supplier<T> sync,
            final Consumer<SingleResultCallback<T>> async) {
        // run the variation-trying code twice, with direct/indirect exceptions
        for (int i = 0; i < 2; i++) {
            throwExceptionsFromAsync = i == 0;

            // the variation-trying code:
            invocationTracker.reset();
            do {
                invocationTracker.startInitialStep();
                assertBehavesSame(
                        sync,
                        () -> invocationTracker.startMatchStep(),
                        async);
            } while (invocationTracker.countDown());
            assertEquals(expectedVariations, invocationTracker.getVariationCount(),
                    "number of variations did not match");
        }

    }

    private <T> void assertBehavesSame(final Supplier<T> sync, final Runnable between, final Consumer<SingleResultCallback<T>> async) {

        T expectedValue = null;
        Throwable expectedException = null;
        try {
            expectedValue = sync.get();
        } catch (Throwable e) {
            expectedException = e;
        }
        List<String> expectedEvents = listener.getEventStrings();

        listener.clear();
        between.run();

        AtomicReference<T> actualValue = new AtomicReference<>();
        AtomicReference<Throwable> actualException = new AtomicReference<>();
        AtomicBoolean wasCalled = new AtomicBoolean(false);
        try {
            async.accept((v, e) -> {
                actualValue.set(v);
                actualException.set(e);
                wasCalled.set(true);
            });
        } catch (Throwable e) {
            fail("async threw instead of using callback");
        }

        // The following code can be used to debug variations:
//        System.out.println("===START");
//        System.out.println("sync: " + expectedEvents);
//        System.out.println("callback called?: " + wasCalled.get());
//        System.out.println("value -- sync: " + expectedValue + " -- async: " + actualValue.get());
//        System.out.println("excep -- sync: " + expectedException + " -- async: " + actualException.get());
//        System.out.println("variant: " + (throwExceptionsFromAsync
//             ? "exceptions thrown directly" : "exceptions into callbacks"));
//        System.out.println("===END");

        assertTrue(wasCalled.get(), "callback should have been called");
        assertEquals(expectedEvents, listener.getEventStrings(), "steps should have matched");
        assertEquals(expectedValue, actualValue.get());
        assertEquals(expectedException == null, actualException.get() == null,
                "both or neither should have produced an exception");
        if (expectedException != null) {
            assertEquals(expectedException.getMessage(), actualException.get().getMessage());
            assertEquals(expectedException.getClass(), actualException.get().getClass());
        }

        listener.clear();
    }

    /**
     * Tracks invocations: allows testing of all variations of a method calls
     */
    private static class InvocationTracker {
        public static final int DEPTH_LIMIT = 50;
        private final List<Integer> invocationResults = new ArrayList<>();
        private boolean isMatchStep = false; // vs initial step
        private int item = 0;
        private int variationCount = 0;

        public void reset() {
            variationCount = 0;
        }

        public void startInitialStep() {
            variationCount++;
            isMatchStep = false;
            item = -1;
        }

        public int getNextOption(final int myOptionsSize) {
            item++;
            if (item >= invocationResults.size()) {
                if (isMatchStep) {
                    fail("result should have been pre-initialized: steps may not match");
                }
                if (isWithinDepthLimit()) {
                    invocationResults.add(myOptionsSize - 1);
                } else {
                    invocationResults.add(0); // choose "0" option, usually an exception
                }
            }
            return invocationResults.get(item);
        }

        public void startMatchStep() {
            isMatchStep = true;
            item = -1;
        }

        private boolean countDown() {
            while (!invocationResults.isEmpty()) {
                int lastItemIndex = invocationResults.size() - 1;
                int lastItem = invocationResults.get(lastItemIndex);
                if (lastItem > 0) {
                    // count current digit down by 1, until 0
                    invocationResults.set(lastItemIndex, lastItem - 1);
                    return true;
                } else {
                    // current digit completed, remove (move left)
                    invocationResults.remove(lastItemIndex);
                }
            }
            return false;
        }

        public int getVariationCount() {
            return variationCount;
        }

        public boolean isWithinDepthLimit() {
            return invocationResults.size() < DEPTH_LIMIT;
        }
    }
}
