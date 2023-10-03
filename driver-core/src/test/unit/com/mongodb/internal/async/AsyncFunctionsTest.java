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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

final class AsyncFunctionsTest {
    private final TestListener listener = new TestListener();
    private final InvocationTracker invocationTracker = new InvocationTracker();

    @Test
    void testVariations1() {
        /*
        In our async code:
        1. a callback is provided as a method parameter
        2. at least one sync method must be converted to async

        To use this API:
        1. start an async chain using the "beginAsync" static method
        2. use an appropriate chaining method (then...), which will provide "c"
        3. copy all sync code to that method
        4. at the async method, pass in "c" and start a new chaining method
        5. provide the original "callback" at the end of the chain via "finish"

        Async methods MUST be preceded by unaffected "plain" sync code (sync
        code with no async counterpart), and this code MUST reside above the
        affected method, as it appears in the sync code. Plain code after
        the sync method should be supplied via one of the "finally" variants.
        Safe "shared" plain code (variable and lambda declarations) which cannot
        throw, may remain outside the chained invocations, for convenience.

        Plain sync code MAY throw exceptions, and SHOULD NOT attempt to handle
        them asynchronously. The exceptions will be caught and handled by the
        chaining methods that contain this sync code.

        Each async lambda MUST invoke its async method with "c", and MUST return
        immediately after invoking that method. It MUST NOT, for example, have
        a catch or finally (including close on try-with-resources) after the
        invocation of the sync method.

        A braced lambda body (with no linebreak before "."), as shown below,
        should be used, as this will be consistent with other usages, and allows
        the async code to be more easily compared to the sync code.
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
    }

    @Test
    void testVariations2() {
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
                    // ...it is moved to its own block
                    beginAsync().thenRun(c -> {
                        async(1, c);
                    }).thenRunAndFinish(() -> {
                        plain(2);
                    }, callback);
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
    void testVariations4() {
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

    @SuppressWarnings("ConstantConditions")
    @Test
    void testFullChain() {
        // tests a chain: runnable, producer, function, function, consumer

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
    void testVariationsBranching() {
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
    void testErrorIf() {
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
                    }).onErrorIf(e -> e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1"), c -> {
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
                    }).onErrorIf(e -> e.getMessage().equals(plainTest(1) ? "unexpected" : "exception-1"), c -> {
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
        try {
            sync(i);
            callback.onResult(null, null);
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private void asyncReturns(final int i, final SingleResultCallback<Integer> callback) {
        try {
            callback.onResult(syncReturns(i), null);
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    // assert methods:

    private void assertBehavesSameVariations(final int expectedVariations, final Runnable sync,
            final Consumer<SingleResultCallback<Void>> async) {
        assertBehavesSameVariations(
                expectedVariations,
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
        invocationTracker.reset();
        do {
            invocationTracker.startInitialStep();
            assertBehavesSame(
                    sync,
                    () -> invocationTracker.startMatchStep(),
                    async);

        } while (invocationTracker.countDown());
        assertEquals(expectedVariations, invocationTracker.getVariationCount());
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
        try {
            async.accept((v, e) -> {
                actualValue.set(v);
                actualException.set(e);
            });
        } catch (Throwable e) {
            fail("async threw instead of using callback");
        }

        // The following code can be used to debug variations:
        // System.out.println("===");
        // System.out.println(listener.getEventStrings());
        // System.out.println("===");

        assertEquals(expectedEvents, listener.getEventStrings(), "steps did not match");
        assertEquals(expectedValue, actualValue.get());
        assertEquals(expectedException == null, actualException.get() == null);
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
