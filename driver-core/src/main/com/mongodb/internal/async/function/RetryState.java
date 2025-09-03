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

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;

/**
 * Represents both the state associated with a retryable activity and a handle that can be used to affect retrying, e.g.,
 * to {@linkplain #breakAndThrowIfRetryAnd(Supplier) break} it.
 * {@linkplain #attachment(AttachmentKey) Attachments} may be used by the associated retryable activity either
 * to preserve a state between attempts.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @see RetryingSyncSupplier
 * @see RetryingAsyncCallbackSupplier
 */
@NotThreadSafe
public final class RetryState {
    public static final int RETRIES = 1;
    private static final int INFINITE_ATTEMPTS = Integer.MAX_VALUE;

    private final LoopState loopState;
    private final int attempts;
    private final boolean retryUntilTimeoutThrowsException;
    @Nullable
    private Throwable previouslyChosenException;

    /**
     * Creates a {@code RetryState} with a positive number of allowed retries. {@link Integer#MAX_VALUE} is a special value interpreted as
     * being unlimited.
     * <p>
     * If a timeout is not specified in the {@link TimeoutContext#hasTimeoutMS()}, the specified {@code retries} param acts as a fallback
     * bound. Otherwise, retries are unbounded until the timeout is reached.
     * <p>
     * It is possible to provide an additional {@code retryPredicate} in the {@link #doAdvanceOrThrow} method,
     * which can be used to stop retrying based on a custom condition additionally to {@code retires} and {@link TimeoutContext}.
     * </p>
     *
     * @param retries A positive number of allowed retries. {@link Integer#MAX_VALUE} is a special value interpreted as being unlimited.
     * @param timeoutContext A timeout context that will be used to determine if the operation has timed out.
     * @see #attempts()
     */
    public static RetryState withRetryableState(final int retries, final TimeoutContext timeoutContext) {
        assertTrue(retries > 0);
        if (timeoutContext.hasTimeoutMS()){
            return new RetryState(INFINITE_ATTEMPTS, timeoutContext);
        }
        return new RetryState(retries, null);
    }

    public static RetryState withNonRetryableState() {
        return new RetryState(0, null);
    }

    /**
     * Creates a {@link RetryState} that does not limit the number of retries.
     * The number of attempts is limited iff {@link TimeoutContext#hasTimeoutMS()} is true and timeout has expired.
     * <p>
     * It is possible to provide an additional {@code retryPredicate} in the {@link #doAdvanceOrThrow} method,
     * which can be used to stop retrying based on a custom condition additionally to {@code retires} and {@link TimeoutContext}.
     * </p>
     *
     * @param timeoutContext A timeout context that will be used to determine if the operation has timed out.
     * @see #attempts()
     */
    public RetryState(final TimeoutContext timeoutContext) {
        this(INFINITE_ATTEMPTS, timeoutContext);
    }

    /**
     * @param retries A non-negative number of allowed retries. {@link Integer#MAX_VALUE} is a special value interpreted as being unlimited.
     * @param timeoutContext A timeout context that will be used to determine if the operation has timed out.
     * @see #attempts()
     */
    private RetryState(final int retries, @Nullable final TimeoutContext timeoutContext) {
        assertTrue(retries >= 0);
        loopState = new LoopState();
        attempts = retries == INFINITE_ATTEMPTS ? INFINITE_ATTEMPTS : retries + 1;
        this.retryUntilTimeoutThrowsException = timeoutContext != null && timeoutContext.hasTimeoutMS();
    }

    /**
     * Advances this {@link RetryState} such that it represents the state of a new attempt.
     * If there is at least one more {@linkplain #attempts() attempt} left, it is consumed by this method.
     * Must not be called before the {@linkplain #isFirstAttempt() first attempt}, must be called before each subsequent attempt.
     * <p>
     * This method is intended to be used by code that generally does not handle {@link Error}s explicitly,
     * which is usually synchronous code.
     *
     * @param attemptException The exception produced by the most recent attempt.
     * It is passed to the {@code retryPredicate} and to the {@code onAttemptFailureOperator}.
     * @param onAttemptFailureOperator The action that is called once per failed attempt before (in the happens-before order) the
     * {@code retryPredicate}, regardless of whether the {@code retryPredicate} is called.
     * This action is allowed to have side effects.
     * <p>
     * It also has to choose which exception to preserve as a prospective failed result of the associated retryable activity.
     * The {@code onAttemptFailureOperator} may mutate its arguments, choose from the arguments, or return a different exception,
     * but it must return a {@code @}{@link NonNull} value.
     * The choice is between</p>
     * <ul>
     *     <li>the previously chosen exception or {@code null} if none has been chosen
     *     (the first argument of the {@code onAttemptFailureOperator})</li>
     *     <li>and the exception from the most recent attempt (the second argument of the {@code onAttemptFailureOperator}).</li>
     * </ul>
     * The result of the {@code onAttemptFailureOperator} does not affect the exception passed to the {@code retryPredicate}.
     * @param retryPredicate {@code true} iff another attempt needs to be made. The {@code retryPredicate} is called not more than once
     * per attempt and only if all the following is true:
     * <ul>
     *     <li>{@code onAttemptFailureOperator} completed normally;</li>
     *     <li>the most recent attempt is not the {@linkplain #isLastAttempt() last} one.</li>
     * </ul>
     * The {@code retryPredicate} accepts this {@link RetryState} and the exception from the most recent attempt,
     * and may mutate the exception. The {@linkplain RetryState} advances to represent the state of a new attempt
     * after (in the happens-before order) testing the {@code retryPredicate}, and only if the predicate completes normally.
     * @throws RuntimeException Iff any of the following is true:
     * <ul>
     *     <li>the {@code onAttemptFailureOperator} completed abruptly;</li>
     *     <li>the most recent attempt is the {@linkplain #isLastAttempt() last} one;</li>
     *     <li>the {@code retryPredicate} completed abruptly;</li>
     *     <li>the {@code retryPredicate} is {@code false}.</li>
     * </ul>
     * The exception thrown represents the failed result of the associated retryable activity,
     * i.e., the caller must not do any more attempts.
     * @see #advanceOrThrow(Throwable, BinaryOperator, BiPredicate)
     */
    void advanceOrThrow(final RuntimeException attemptException, final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate) throws RuntimeException {
        try {
            doAdvanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate, true);
        } catch (RuntimeException | Error unchecked) {
            throw unchecked;
        } catch (Throwable checked) {
            throw new AssertionError(checked);
        }
    }

    /**
     * This method is intended to be used by code that generally handles all {@link Throwable} types explicitly,
     * which is usually asynchronous code.
     *
     * @see #advanceOrThrow(RuntimeException, BinaryOperator, BiPredicate)
     */
    void advanceOrThrow(final Throwable attemptException, final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate) throws Throwable {
        doAdvanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate, false);
    }

    /**
     * @param onlyRuntimeExceptions {@code true} iff the method must expect {@link #previouslyChosenException} and {@code attemptException} to be
     * {@link RuntimeException}s and must not explicitly handle other {@link Throwable} types, of which only {@link Error} is possible
     * as {@link RetryState} does not have any source of {@link Exception}s.
     * @param onAttemptFailureOperator See {@link #advanceOrThrow(RuntimeException, BinaryOperator, BiPredicate)}.
     */
    private void doAdvanceOrThrow(final Throwable attemptException,
            final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate,
            final boolean onlyRuntimeExceptions) throws Throwable {
        assertTrue(attempt() < attempts);
        assertNotNull(attemptException);
        if (onlyRuntimeExceptions) {
            assertTrue(isRuntime(attemptException));
        }
        assertTrue(!isFirstAttempt() || previouslyChosenException == null);
        Throwable newlyChosenException = callOnAttemptFailureOperator(previouslyChosenException, attemptException, onlyRuntimeExceptions, onAttemptFailureOperator);

        /*
         * A MongoOperationTimeoutException indicates that the operation timed out, either during command execution or server selection.
         * The timeout for server selection is determined by the computedServerSelectionMS = min(serverSelectionTimeoutMS, timeoutMS).
         *
         * It is important to check if the exception is an instance of MongoOperationTimeoutException to detect a timeout.
         */
        if (isLastAttempt() || attemptException instanceof MongoOperationTimeoutException) {
            previouslyChosenException = newlyChosenException;
            /*
             * The function of isLastIteration() is to indicate if retrying has
             * been explicitly halted. Such a stop is not interpreted as
             * a timeout exception but as a deliberate cessation of retry attempts.
             */
            if (retryUntilTimeoutThrowsException && !loopState.isLastIteration()) {
                previouslyChosenException = createMongoTimeoutException(
                        "Retry attempt exceeded the timeout limit.",
                        previouslyChosenException);
            }
            throw previouslyChosenException;
        } else {
            // note that we must not update the state, e.g, `previouslyChosenException`, `loopState`, before calling `retryPredicate`
            boolean retry = shouldRetry(this, attemptException, newlyChosenException, onlyRuntimeExceptions, retryPredicate);
            previouslyChosenException = newlyChosenException;
            if (retry) {
                assertTrue(loopState.advance());
            } else {
                throw previouslyChosenException;
            }
        }
    }

    /**
     * @param onlyRuntimeExceptions See {@link #doAdvanceOrThrow(Throwable, BinaryOperator, BiPredicate, boolean)}.
     * @param onAttemptFailureOperator See {@link #advanceOrThrow(RuntimeException, BinaryOperator, BiPredicate)}.
     */
    private static Throwable callOnAttemptFailureOperator(
            @Nullable final Throwable previouslyChosenException,
            final Throwable attemptException,
            final boolean onlyRuntimeExceptions,
            final BinaryOperator<Throwable> onAttemptFailureOperator) {
        if (onlyRuntimeExceptions && previouslyChosenException != null) {
            assertTrue(isRuntime(previouslyChosenException));
        }
        Throwable result;
        try {
            result = assertNotNull(onAttemptFailureOperator.apply(previouslyChosenException, attemptException));
            if (onlyRuntimeExceptions) {
                assertTrue(isRuntime(result));
            }
        } catch (Throwable onAttemptFailureOperatorException) {
            if (onlyRuntimeExceptions && !isRuntime(onAttemptFailureOperatorException)) {
                throw onAttemptFailureOperatorException;
            }
            if (previouslyChosenException != null) {
                onAttemptFailureOperatorException.addSuppressed(previouslyChosenException);
            }
            onAttemptFailureOperatorException.addSuppressed(attemptException);
            throw onAttemptFailureOperatorException;
        }
        return result;
    }

    /**
     * @param readOnlyRetryState Must not be mutated by this method.
     * @param onlyRuntimeExceptions See {@link #doAdvanceOrThrow(Throwable, BinaryOperator, BiPredicate, boolean)}.
     */
    private boolean shouldRetry(final RetryState readOnlyRetryState, final Throwable attemptException, final Throwable newlyChosenException,
            final boolean onlyRuntimeExceptions, final BiPredicate<RetryState, Throwable> retryPredicate) {
        try {
            return retryPredicate.test(readOnlyRetryState, attemptException);
        } catch (Throwable retryPredicateException) {
            if (onlyRuntimeExceptions && !isRuntime(retryPredicateException)) {
                throw retryPredicateException;
            }
            retryPredicateException.addSuppressed(newlyChosenException);
            throw retryPredicateException;
        }
    }

    private static boolean isRuntime(@Nullable final Throwable exception) {
        return exception instanceof RuntimeException;
    }

    /**
     * This method is similar to the semantics of the
     * <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.15">{@code break} statement</a>, with the difference
     * that breaking results in throwing an exception because the retry loop has more than one iteration only if the first iteration fails.
     * Does nothing and completes normally if called during the {@linkplain #isFirstAttempt() first attempt}.
     * This method is useful when the associated retryable activity detects that a retry attempt should not happen
     * despite having been started. Must not be called more than once per {@link RetryState}.
     * <p>
     * If the {@code predicate} completes abruptly, this method also completes abruptly with the same exception but does not break retrying;
     * if the {@code predicate} is {@code true}, then the method breaks retrying and completes abruptly by throwing the exception that is
     * currently deemed to be a prospective failed result of the associated retryable activity. The thrown exception must also be used
     * by the caller to complete the ongoing attempt.
     * <p>
     * If this method is called from
     * {@linkplain RetryingSyncSupplier#RetryingSyncSupplier(RetryState, BinaryOperator, BiPredicate, Supplier)
     * retry predicate / failed result transformer}, the behavior is unspecified.
     *
     * @param predicate {@code true} iff retrying needs to be broken.
     * The {@code predicate} is not called during the {@linkplain #isFirstAttempt() first attempt}.
     * @throws RuntimeException Iff any of the following is true:
     * <ul>
     *     <li>the {@code predicate} completed abruptly;</li>
     *     <li>this method broke retrying.</li>
     * </ul>
     * The exception thrown represents the failed result of the associated retryable activity.
     * @see #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)
     */
    public void breakAndThrowIfRetryAnd(final Supplier<Boolean> predicate) throws RuntimeException {
        assertFalse(loopState.isLastIteration());
        if (!isFirstAttempt()) {
            assertNotNull(previouslyChosenException);
            assertTrue(previouslyChosenException instanceof RuntimeException);
            RuntimeException localException = (RuntimeException) previouslyChosenException;
            try {
                if (predicate.get()) {
                    loopState.markAsLastIteration();
                }
            } catch (Exception predicateException) {
                predicateException.addSuppressed(localException);
                throw predicateException;
            }
            if (loopState.isLastIteration()) {
                throw localException;
            }
        }
    }

    /**
     * This method is intended to be used by callback-based code. It is similar to {@link #breakAndThrowIfRetryAnd(Supplier)},
     * but instead of throwing an exception, it relays it to the {@code callback}.
     * <p>
     * If this method is called from
     * {@linkplain RetryingAsyncCallbackSupplier#RetryingAsyncCallbackSupplier(RetryState, BinaryOperator, BiPredicate, AsyncCallbackSupplier)
     * retry predicate / failed result transformer}, the behavior is unspecified.
     *
     * @return {@code true} iff the {@code callback} was completed, which happens in the same situations in which
     * {@link #breakAndThrowIfRetryAnd(Supplier)} throws an exception. If {@code true} is returned, the caller must complete
     * the ongoing attempt.
     * @see #breakAndThrowIfRetryAnd(Supplier)
     */
    public boolean breakAndCompleteIfRetryAnd(final Supplier<Boolean> predicate, final SingleResultCallback<?> callback) {
        try {
            breakAndThrowIfRetryAnd(predicate);
            return false;
        } catch (Throwable t) {
            callback.onResult(null, t);
            return true;
        }
    }

    /**
     * This method is similar to
     * {@link RetryState#breakAndThrowIfRetryAnd(Supplier)} / {@link RetryState#breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)}.
     * The difference is that it allows the current attempt to continue, yet no more attempts will happen. Also, unlike the aforementioned
     * methods, this method has effect even if called during the {@linkplain #isFirstAttempt() first attempt}.
     */
    public void markAsLastAttempt() {
        loopState.markAsLastIteration();
    }

    /**
     * Returns {@code true} iff the current attempt is the first one, i.e., no retries have been made.
     *
     * @see #attempts()
     */
    public boolean isFirstAttempt() {
        return loopState.isFirstIteration();
    }

    /**
     * Returns {@code true} iff the current attempt is known to be the last one, i.e., it is known that no more retries will be made.
     * An attempt is known to be the last one iff any of the following applies:
     * <ul>
     *   <li>{@link #breakAndThrowIfRetryAnd(Supplier)} / {@link #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)} / {@link #markAsLastAttempt()} was called.</li>
     *   <li>A timeout is set and has been reached.</li>
     *   <li>No timeout is set, and the number of {@linkplain #attempts() attempts} is limited, and the current attempt is the last one.</li>
     * </ul>
     *
     * @see #attempts()
     */
    public boolean isLastAttempt() {
        if (loopState.isLastIteration()){
            return true;
        }
        if (retryUntilTimeoutThrowsException) {
           return false;
        }
        return attempt() == attempts - 1;
    }

    /**
     * A 0-based attempt number.
     *
     * @see #attempts()
     */
    public int attempt() {
        return loopState.iteration();
    }

    /**
     * Returns a positive maximum number of attempts:
     * <ul>
     *     <li>0 if the number of retries is {@linkplain #RetryState(TimeoutContext) unlimited};</li>
     *     <li>1 if no retries are allowed;</li>
     *     <li>{@link #RetryState(int, TimeoutContext) retries} + 1 otherwise.</li>
     * </ul>
     *
     * @see #attempt()
     * @see #isFirstAttempt()
     * @see #isLastAttempt()
     */
    public int attempts() {
        return attempts == INFINITE_ATTEMPTS ? 0 : attempts;
    }

    /**
     * Returns the exception that is currently deemed to be a prospective failed result of the associated retryable activity.
     * Note that this exception is not necessary the one from the most recent failed attempt.
     * Returns an {@linkplain Optional#isEmpty() empty} {@link Optional} iff called during the {@linkplain #isFirstAttempt() first attempt}.
     * <p>
     * In synchronous code the returned exception is of the type {@link RuntimeException}.
     */
    public Optional<Throwable> exception() {
        assertTrue(previouslyChosenException == null || !isFirstAttempt());
        return Optional.ofNullable(previouslyChosenException);
    }

    /**
     * @see LoopState#attach(AttachmentKey, Object, boolean)
     */
    public <V> RetryState attach(final AttachmentKey<V> key, final V value, final boolean autoRemove) {
        loopState.attach(key, value, autoRemove);
        return this;
    }

    /**
     * @see LoopState#attachment(AttachmentKey)
     */
    public <V> Optional<V> attachment(final AttachmentKey<V> key) {
        return loopState.attachment(key);
    }

    @Override
    public String toString() {
        return "RetryState{"
                + "loopState=" + loopState
                + ", attempts=" + (attempts == INFINITE_ATTEMPTS ? "infinite" : attempts)
                + ", exception=" + previouslyChosenException
                + '}';
    }
}
