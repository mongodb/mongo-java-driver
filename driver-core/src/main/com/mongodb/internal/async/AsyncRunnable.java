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

import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * <p>See the test code (AsyncFunctionsTest) for API usage.
 *
 * <p>This API is used to write "Async" methods. These must exhibit the
 * same behaviour as their sync counterparts, except asynchronously,
 * and will make use of a {@link SingleResultCallback} parameter.
 *
 * <p>This API makes it easy to compare and verify async code against
 * corresponding sync code, since the "shape" and ordering of the
 * async code matches that of the sync code. For example, given the
 * following "sync" method:
 *
 * <pre>
 * public T myMethod()
 *     method1();
 *     method2();
 * }</pre>
 *
 * <p>The async counterpart would be:
 *
 * <pre>
 * public void myMethodAsync(SingleResultCallback&lt;T> callback)
 *     beginAsync().thenRun(c -> {
 *         method1Async(c);
 *     }).thenRun(c -> {
 *         method2Async(c);
 *     }).finish(callback);
 * }
 * </pre>
 *
 * <p>The usage of this API is defined in its tests (AsyncFunctionsTest).
 * Each test specifies the Async API code that must be used to formally
 * replace a particular pattern of sync code. These tests, in a sense,
 * define formal rules of replacement.
 *
 * <p>Requirements and conventions:
 *
 * <p>Each async method SHOULD start with {@link #beginAsync()}, which begins
 * a chain of lambdas. Each lambda provides a callback "c" that MUST be passed
 * or completed at the lambda's end of execution. The async method's "callback"
 * parameter MUST be passed to {@link #finish(SingleResultCallback)}, and MUST
 * NOT be used otherwise.
 *
 * <p>Consider refactoring corresponding sync code to reduce nesting or to
 * otherwise improve clarity, since minor issues will often be amplified in
 * the async code.
 *
 * <p>Each async lambda MUST invoke its async method with "c", and MUST return
 * immediately after invoking that method. It MUST NOT, for example, have
 * a catch or finally (including close on try-with-resources) after the
 * invocation of the async method.
 *
 * <p>In cases where the async method has "mixed" returns (some of which are
 * plain sync, some async), the "c" callback MUST be completed on the
 * plain sync path, using {@link SingleResultCallback#complete(Object)} or
 * {@link SingleResultCallback#complete(SingleResultCallback)}, followed by a
 * return or end of method.
 *
 * <p>Chains starting with {@link #beginAsync()} correspond roughly to code
 * blocks. This includes method bodies, blocks used in if/try/catch/while/etc.
 * statements, and places where anonymous code blocks might be used. For
 * clarity, such nested/indented chains might be omitted (where possible,
 * as demonstrated in tests).
 *
 * <p>Plain sync code MAY throw exceptions, and SHOULD NOT attempt to handle
 * them asynchronously. The exceptions will be caught and handled by the API.
 *
 * <p>All code, including "plain" code (parameter checks) SHOULD be placed
 * within the API's async lambdas. This ensures that exceptions are handled,
 * and facilitates comparison/review. This excludes code that must be
 * "shared", such as lambda and variable declarations.
 *
 * <p>For consistency, and ease of comparison/review, async chains SHOULD be
 * formatted as in the tests; that is, with line-breaks at the curly-braces of
 * lambda bodies, with no linebreak before the "." of any Async API method.
 *
 * <p>Code review checklist, for common mistakes:
 *
 * <ol>
 *   <li>Is everything (that can be) inside the async lambdas?</li>
 *   <li>Is "callback" supplied to "finish"?</li>
 *   <li>In each block and nested block, is that same block's "c" always
 *   passed/completed at the end of execution?</li>
 *   <li>Is every c.complete followed by a return, to end execution?</li>
 *   <li>Have all sync method calls been converted to async, where needed?</li>
 * </ol>
 *
 * <p>This class is not part of the public API and may be removed or changed
 * at any time
 */
@FunctionalInterface
public interface AsyncRunnable extends AsyncSupplier<Void>, AsyncConsumer<Void> {

    static AsyncRunnable beginAsync() {
        return (c) -> c.complete(c);
    }

    /**
     * Must be invoked at end of async chain
     * @param runnable the sync code to invoke (under non-exceptional flow)
     *                 prior to the callback
     * @param callback the callback provided by the method the chain is used in
     */
    default void thenRunAndFinish(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.finish((r, e) -> {
            if (e != null) {
                callback.completeExceptionally(e);
                return;
            }
            try {
                runnable.run();
            } catch (Throwable t) {
                callback.completeExceptionally(t);
                return;
            }
            callback.complete(callback);
        });
    }

    /**
     * See {@link #thenRunAndFinish(Runnable, SingleResultCallback)}, but the runnable
     * will always be executed, including on the exceptional path.
     * @param runnable the runnable
     * @param callback the callback
     */
    default void thenAlwaysRunAndFinish(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.finish((r, e) -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                if (e != null) {
                    t.addSuppressed(e);
                }
                callback.completeExceptionally(t);
                return;
            }
            callback.onResult(r, e);
        });
    }

    /**
     * @param runnable The async runnable to run after this runnable
     * @return the composition of this runnable and the runnable, a runnable
     */
    default AsyncRunnable thenRun(final AsyncRunnable runnable) {
        return (c) -> {
            this.unsafeFinish((r, e) -> {
                if (e == null) {
                    /* If 'runnable' is executed on a different thread from the one that executed the initial 'finish()',
                     then invoking 'finish()' within 'runnable' will catch and propagate any exceptions to 'c' (the callback). */
                    runnable.finish(c);
                } else {
                    c.completeExceptionally(e);
                }
            });
        };
    }

    /**
     * @param condition the condition to check
     * @param runnable The async runnable to run after this runnable,
     *                 if and only if the condition is met
     * @return the composition of this runnable and the runnable, a runnable
     */
    default AsyncRunnable thenRunIf(final Supplier<Boolean> condition, final AsyncRunnable runnable) {
        return (callback) -> {
            this.unsafeFinish((r, e) -> {
                if (e != null) {
                    callback.completeExceptionally(e);
                    return;
                }
                boolean matched;
                try {
                    matched = condition.get();
                } catch (Throwable t) {
                    callback.completeExceptionally(t);
                    return;
                }
                if (matched) {
                    runnable.finish(callback);
                } else {
                    callback.complete(callback);
                }
            });
        };
    }

    /**
     * @param supplier The supplier to supply using after this runnable
     * @return the composition of this runnable and the supplier, a supplier
     * @param <R> The return type of the resulting supplier
     */
    default <R> AsyncSupplier<R> thenSupply(final AsyncSupplier<R> supplier) {
        return (c) -> {
            this.unsafeFinish((r, e) -> {
                if (e == null) {
                    supplier.finish(c);
                } else {
                    c.completeExceptionally(e);
                }
            });
        };
    }

    /**
     * @param runnable    the runnable to loop
     * @param shouldRetry condition under which to retry
     * @return the composition of this, and the looping branch
     * @see RetryingAsyncCallbackSupplier
     */
    default AsyncRunnable thenRunRetryingWhile(
            final AsyncRunnable runnable, final Predicate<Throwable> shouldRetry) {
        return thenRun(callback -> {
            new RetryingAsyncCallbackSupplier<Void>(
                    new RetryState(),
                    (rs, lastAttemptFailure) -> shouldRetry.test(lastAttemptFailure),
                    // `finish` is required here instead of `unsafeFinish`
                    // because only `finish` meets the contract of
                    // `AsyncCallbackSupplier.get`, which we implement here
                    cb -> runnable.finish(cb)
            ).get(callback);
        });
    }
}
