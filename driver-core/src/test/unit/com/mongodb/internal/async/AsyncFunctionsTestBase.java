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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class AsyncFunctionsTestBase {

    private final TestListener listener = new TestListener();
    private final InvocationTracker invocationTracker = new InvocationTracker();
    private boolean isTestingAbruptCompletion = false;
    private ExecutorService asyncExecutor;

    void setIsTestingAbruptCompletion(final boolean b) {
        isTestingAbruptCompletion = b;
    }

    public void setAsyncStep(final boolean isAsyncStep) {
        invocationTracker.isAsyncStep = isAsyncStep;
    }

    public void getNextOption(final int i) {
        invocationTracker.getNextOption(i);
    }

    public void listenerAdd(final String s) {
        listener.add(s);
    }

    /**
     * Create an executor service for async operations before each test.
     *
     * @return the executor service.
     */
    public abstract ExecutorService createAsyncExecutor();

    @BeforeEach
    public void setUp() {
        asyncExecutor = createAsyncExecutor();
    }

    @AfterEach
    public void shutDown() {
        asyncExecutor.shutdownNow();
    }

    void plain(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("plain-exception-" + i);
            throw new RuntimeException("affected method exception-" + i);
        } else {
            listener.add("plain-success-" + i);
        }
    }

    int plainReturns(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("plain-returns-exception-" + i);
            throw new RuntimeException("affected method exception-" + i);
        } else {
            listener.add("plain-returns-success-" + i);
            return i;
        }
    }

    boolean plainTest(final int i) {
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

    void sync(final int i) {
        assertFalse(invocationTracker.isAsyncStep);
        affected(i);
    }

    Integer syncReturns(final int i) {
        assertFalse(invocationTracker.isAsyncStep);
        return affectedReturns(i);
    }


    public void submit(final Runnable task) {
        asyncExecutor.execute(task);
    }
    void async(final int i, final SingleResultCallback<Void> callback) {
        assertTrue(invocationTracker.isAsyncStep);
        if (isTestingAbruptCompletion) {
            /* We should not test for abrupt completion in a separate thread. Once a callback is registered for an async operation,
            the Async Framework does not handle exceptions thrown outside of callbacks by the executing thread. Such exception management
            should be the responsibility of the thread conducting the asynchronous operations. */
            affected(i);
            submit(() -> {
                callback.complete(callback);
            });
        } else {
            submit(() -> {
                try {
                    affected(i);
                    callback.complete(callback);
                } catch (Throwable t) {
                    callback.onResult(null, t);
                }
            });
        }
    }

    void asyncReturns(final int i, final SingleResultCallback<Integer> callback) {
        assertTrue(invocationTracker.isAsyncStep);
        if (isTestingAbruptCompletion) {
            int result = affectedReturns(i);
            submit(() -> {
                callback.complete(result);
            });
        } else {
            submit(() -> {
                try {
                    callback.complete(affectedReturns(i));
                } catch (Throwable t) {
                    callback.onResult(null, t);
                }
            });
        }
    }

    private void affected(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("affected-exception-" + i);
            throw new RuntimeException("exception-" + i);
        } else {
            listener.add("affected-success-" + i);
        }
    }

    private int affectedReturns(final int i) {
        int cur = invocationTracker.getNextOption(2);
        if (cur == 0) {
            listener.add("affected-returns-exception-" + i);
            throw new RuntimeException("exception-" + i);
        } else {
            listener.add("affected-returns-success-" + i);
            return i;
        }
    }

    // assert methods:

    void assertBehavesSameVariations(final int expectedVariations, final Runnable sync,
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

    <T> void assertBehavesSameVariations(final int expectedVariations, final Supplier<T> sync,
                                         final Consumer<SingleResultCallback<T>> async) {
        // run the variation-trying code twice, with direct/indirect exceptions
        for (int i = 0; i < 2; i++) {
            isTestingAbruptCompletion = i != 0;

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

    private <T> void assertBehavesSame(final Supplier<T> sync, final Runnable between,
                                       final Consumer<SingleResultCallback<T>> async) {

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
        CompletableFuture<Void> wasCalledFuture = new CompletableFuture<>();
        try {
            async.accept((v, e) -> {
                actualValue.set(v);
                actualException.set(e);
                if (wasCalledFuture.isDone()) {
                    fail();
                }
                wasCalledFuture.complete(null);
            });
        } catch (Throwable e) {
            fail("async threw instead of using callback");
        }

        await(wasCalledFuture, "Callback should have been called");

        // The following code can be used to debug variations:
//        System.out.println("===VARIATION START");
//        System.out.println("sync: " + expectedEvents);
//        System.out.println("callback called?: " + wasCalledFuture.isDone());
//        System.out.println("value -- sync: " + expectedValue + " -- async: " + actualValue.get());
//        System.out.println("excep -- sync: " + expectedException + " -- async: " + actualException.get());
//        System.out.println("exception mode: " + (isTestingAbruptCompletion
//             ? "exceptions thrown directly (abrupt completion)" : "exceptions into callbacks"));
//        System.out.println("===VARIATION END");

        // show assertion failures arising in async tests
        if (actualException.get() != null && actualException.get() instanceof AssertionFailedError) {
            throw (AssertionFailedError) actualException.get();
        }

        assertTrue(wasCalledFuture.isDone(), "callback should have been called");
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

    protected <T> T await(final CompletableFuture<T> voidCompletableFuture, final String errorMessage) {
        try {
            return voidCompletableFuture.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError(errorMessage);
        }
    }

    /**
     * Tracks invocations: allows testing of all variations of a method calls
     */
    static class InvocationTracker {
        public static final int DEPTH_LIMIT = 50;
        private final List<Integer> invocationOptionSequence = new ArrayList<>();
        private boolean isAsyncStep; // async = matching, vs initial step = populating
        private int currentInvocationIndex;
        private int variationCount;

        public void reset() {
            variationCount = 0;
        }

        public void startInitialStep() {
            variationCount++;
            isAsyncStep = false;
            currentInvocationIndex = -1;
        }

        public int getNextOption(final int myOptionsSize) {
            /*
            This method creates (or gets) the next invocation's option. Each
            invoker of this method has the "option" to behave in various ways,
            usually just success (option 1) and exceptional failure (option 0),
            though some callers might have more options. A sequence of method
            outcomes (options) is one "variation". Tests automatically test
            all possible variations (up to a limit, to prevent infinite loops).

            Methods generally have labels, to ensure that corresponding
            sync/async methods are called in the right order, but these labels
            are unrelated to the "variation" logic here. There are two "modes"
            (whether completion is abrupt, or not), which are also unrelated.
             */

            currentInvocationIndex++; // which invocation result we are dealing with

            if (currentInvocationIndex >= invocationOptionSequence.size()) {
                if (isAsyncStep) {
                    fail("result should have been pre-initialized: steps may not match");
                }
                if (isWithinDepthLimit()) {
                    invocationOptionSequence.add(myOptionsSize - 1);
                } else {
                    invocationOptionSequence.add(0); // choose "0" option, should always be an exception
                }
            }
            return invocationOptionSequence.get(currentInvocationIndex);
        }

        public void startMatchStep() {
            isAsyncStep = true;
            currentInvocationIndex = -1;
        }

        private boolean countDown() {
            while (!invocationOptionSequence.isEmpty()) {
                int lastItemIndex = invocationOptionSequence.size() - 1;
                int lastItem = invocationOptionSequence.get(lastItemIndex);
                if (lastItem > 0) {
                    // count current digit down by 1, until 0
                    invocationOptionSequence.set(lastItemIndex, lastItem - 1);
                    return true;
                } else {
                    // current digit completed, remove (move left)
                    invocationOptionSequence.remove(lastItemIndex);
                }
            }
            return false;
        }

        public int getVariationCount() {
            return variationCount;
        }

        public boolean isWithinDepthLimit() {
            return invocationOptionSequence.size() < DEPTH_LIMIT;
        }
    }
}
