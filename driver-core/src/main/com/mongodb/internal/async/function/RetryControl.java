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
package com.mongodb.internal.async.function;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.assertions.Assertions;
import com.mongodb.internal.async.AsyncSupplier;
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.RetryPolicy.Decision;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.lang.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * A stateful controller of a retryable activity that can be used to control it, for example,
 * to {@linkplain #breakAndThrowIfRetryAnd(Supplier) break} it.
 * Either {@linkplain MutableValue} or an implementation of {@link RetryPolicy} may be used by the retryable activity
 * to preserve state between attempts.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see RetryingSyncSupplier
 * @see RetryingAsyncCallbackSupplier
 */
@NotThreadSafe
public final class RetryControl<P extends RetryPolicy> implements RetryContext {
    private final LoopControl loopControl;
    private boolean disabled;
    @Nullable
    private Decision mostRecentDecision;
    private final P policy;

    public RetryControl(final P policy) {
        loopControl = new LoopControl();
        this.policy = policy;
        disabled = false;
        mostRecentDecision = null;
    }

    @Override
    public boolean isFirstAttempt() {
        return loopControl.isFirstIteration();
    }

    @Override
    public int attempt() {
        return loopControl.iteration();
    }

    public P getPolicy() {
        return policy;
    }

    /**
     * Advances this {@link RetryControl} such that it represents the state of the immediate next attempt, if any.
     * Must not be called before the {@linkplain #isFirstAttempt() first attempt}, must be called before each subsequent attempt.
     *
     * @param attemptFailedResult The failed result of the most recent attempt.
     * @return {@link RetryAttemptInfo} iff another attempt must be executed.
     * @throws RuntimeException If another attempt must not be executed.
     * The exception thrown represents the failed result of the retryable activity.
     */
    RetryAttemptInfo advanceOrThrow(final Throwable attemptFailedResult) throws RuntimeException {
        assertNotNull(attemptFailedResult);
        try {
            if (disabled) {
                throw attemptFailedResult;
            }
            // this `RetryControl` must not be mutated before calling `onAttemptFailure`
            Decision decision = onAttemptFailure(policy, this, prospectiveFailedResult(), attemptFailedResult);
            mostRecentDecision = decision;
            if (loopControl.isLastIteration() || !decision.getImmediateNextAttemptInfo().isPresent()) {
                throw decision.getProspectiveFailedResult();
            } else {
                assertTrue(loopControl.advance());
                return decision.getImmediateNextAttemptInfo().orElseThrow(Assertions::fail);
            }
        } catch (RuntimeException | Error uncheckedFailedResult) {
            throw uncheckedFailedResult;
        } catch (Throwable checkedFailedResult) {
            throw new RuntimeException(checkedFailedResult);
        }
    }

    private static <P extends RetryPolicy> Decision onAttemptFailure(
            final P policy,
            final RetryContext retryContext,
            @Nullable final Throwable prospectiveFailedResult,
            final Throwable attemptFailedResult) throws RuntimeException {
        Decision decision;
        try {
            decision = assertNotNull(policy.onAttemptFailure(retryContext, attemptFailedResult));
        } catch (Throwable onAttemptFailureException) {
            if (prospectiveFailedResult != null && prospectiveFailedResult != onAttemptFailureException) {
                onAttemptFailureException.addSuppressed(prospectiveFailedResult);
            }
            if (attemptFailedResult != onAttemptFailureException) {
                onAttemptFailureException.addSuppressed(attemptFailedResult);
            }
            throw onAttemptFailureException;
        }
        return decision;
    }

    @Override
    public Optional<Throwable> getProspectiveFailedResult() {
        assertTrue(isFirstAttempt() ^ mostRecentDecision != null);
        return Optional.ofNullable(prospectiveFailedResult());
    }

    @Nullable
    private Throwable prospectiveFailedResult() {
        return mostRecentDecision == null ? null : mostRecentDecision.getProspectiveFailedResult();
    }

    /**
     * This method is similar to the semantics of the
     * <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.15">{@code break} statement</a>,
     * with the difference that breaking results in throwing an exception.
     * That thrown exception must be used by the caller to complete the current attempt.
     * Does nothing and completes normally if called during the {@linkplain #isFirstAttempt() first attempt}.
     * <p>
     * This method is useful when the retryable activity detects that a retry attempt should not happen
     * despite having been started.
     * <p>
     * Must not be called after breaking the retry loop.
     *
     * @param predicate {@code true} iff the retry loop needs to be broken.
     * The {@code predicate} is not called during the {@linkplain #isFirstAttempt() first attempt}.
     * <ul>
     *  <li>
     *  If the {@code predicate} completes abruptly, this method completes abruptly with the same exception, but does not break the retry loop;</li>
     *  <li>
     *  if the {@code predicate} is {@code true}, then this method breaks the retry loop and completes abruptly by throwing {@link #getProspectiveFailedResult()};</li>
     *  <li>
     *  if the {@code predicate} is {@code false}, then this method does nothing.
     * </ul>
     * @throws RuntimeException Iff any of the following is true:
     * <ul>
     *     <li>the {@code predicate} completed abruptly;</li>
     *     <li>this method broke the retry loop.</li>
     * </ul>
     */
    public void breakAndThrowIfRetryAnd(final Supplier<Boolean> predicate) throws RuntimeException {
        assertFalse(loopControl.isLastIteration());
        if (isFirstAttempt()) {
            return;
        }
        Throwable prospectiveFailedResult = assertNotNull(prospectiveFailedResult());
        try {
            if (predicate.get()) {
                loopControl.markAsLastIteration();
            }
        } catch (Throwable predicateException) {
            if (prospectiveFailedResult != predicateException) {
                predicateException.addSuppressed(prospectiveFailedResult);
            }
            throw predicateException;
        }
        if (loopControl.isLastIteration()) {
            try {
                throw prospectiveFailedResult;
            } catch (RuntimeException | Error unchecked) {
                throw unchecked;
            } catch (Throwable checked) {
                throw new RuntimeException(checked);
            }
        }
    }

    /**
     * This method allows to execute {@code action} within the encompassing retryable activity, as if it were not retryable.
     * If {@code action} throws an exception, then this method throws that same exception, and, if the current attempt fails,
     * the {@link RetryPolicy#onAttemptFailure(RetryContext, Throwable)} is not called, and the failed result of the attempt
     * becomes the failed result of the retryable activity disregarding {@link #getProspectiveFailedResult()}.
     *
     * @see #doWhileDisabledAsync(AsyncSupplier, SingleResultCallback)
     */
    public <R> R doWhileDisabled(final Supplier<R> action) {
        boolean originalDisabled = disabled;
        disabled = true;
        R result = action.get();
        // `disabled` must be reverted to its original value only if `action` completes normally
        disabled = originalDisabled;
        return result;
    }

    /**
     * This method is similar to {@link #doWhileDisabled(Supplier)},
     * but instead of throwing an exception, it completes the {@code callback} with it.
     * This method is intended to be used in callback-based code.
     */
    public <R> void doWhileDisabledAsync(final AsyncSupplier<R> action, final SingleResultCallback<R> callback) {
        boolean originalDisabled = disabled;
        disabled = true;
        beginAsync().<R>thenSupply(c -> {
            action.finish(c);
        }).thenRunAndFinish(() -> {
            // `disabled` must be reverted to its original value only if `action` completes normally
            disabled = originalDisabled;
        }, callback);
    }

    @Override
    public String toString() {
        return "RetryControl{"
                + "loopControl=" + loopControl
                + ", disabled=" + disabled
                + ", mostRecentDecision=" + mostRecentDecision
                + ", policy=" + policy
                + '}';
    }
}
