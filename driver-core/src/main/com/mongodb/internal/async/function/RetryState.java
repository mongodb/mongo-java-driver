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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;

/**
 * Represents both the state associated with a retryable activity and a handle that can be used to affect retrying, e.g.,
 * to {@linkplain #breakAndThrowIfRetryAnd(Supplier) break} it.
 * {@linkplain #attachment(AttachmentKey) Attachments} may be used by the associated retryable activity either
 * to preserve a state between attempts.
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
    @Nullable
    private Throwable exception;

    /**
     * @param retries A non-negative number of allowed retries.
     * @see #attempts()
     */
    public RetryState(final int retries) {
        assertTrue(retries >= 0);
        assertTrue(retries < INFINITE_ATTEMPTS);
        loopState = new LoopState();
        attempts = retries + 1;
    }

    /**
     * Creates a {@link RetryState} that does not limit the number of retries.
     * @see #attempts()
     */
    public RetryState() {
        loopState = new LoopState();
        attempts = INFINITE_ATTEMPTS;
    }

    /**
     * Advances this {@link RetryState} such that it represents the state of a new attempt.
     * If there is at least one more {@linkplain #attempts() attempt} left, it is consumed by this method.
     * Must not be called before the {@linkplain #firstAttempt() first attempt}, must be called before each subsequent attempt.
     * <p>
     * This method is intended to be used by code that generally does not handle {@link Error}s explicitly,
     * which is usually synchronous code.
     *
     * @param attemptException The exception produced by the most recent attempt.
     * It is passed to the {@code retryPredicate} and to the {@code exceptionTransformer}.
     * @param exceptionTransformer A function that chooses which exception to preserve as a prospective failed result of the associated
     * retryable activity and may also transform or mutate the exceptions.
     * The choice is between
     * <ul>
     *     <li>the previously chosen exception or {@code null} if none has been chosen
     *     (the first argument of the {@code exceptionTransformer})</li>
     *     <li>and the exception from the most recent attempt (the second argument of the {@code exceptionTransformer}).</li>
     * </ul>
     * The {@code exceptionTransformer} may either choose from its arguments, or return a different exception, but it must return
     * a {@code @}{@link NonNull} value.
     * The {@code exceptionTransformer} is called once before (in the happens-before order) the {@code retryPredicate},
     * regardless of whether the {@code retryPredicate} is called. The result of the {@code exceptionTransformer} does not affect
     * the arguments passed to the {@code retryPredicate}, but they still may be mutated by the {@code exceptionTransformer}.
     * @param retryPredicate {@code true} iff another attempt needs to be made. The {@code retryPredicate} is called not more than once
     * per attempt and only if all the following is true:
     * <ul>
     *     <li>{@code exceptionTransformer} completed normally;</li>
     *     <li>retrying was broken via
     *     {@link #breakAndThrowIfRetryAnd(Supplier)} / {@link #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)} /
     *     {@link #markAsLastAttempt()};</li>
     *     <li>there is at least one more {@linkplain #attempts() attempt} left.</li>
     * </ul>
     * This {@linkplain RetryState} is updated after (in the happens-before order) testing the {@code retryPredicate},
     * and only if the predicate completes normally.
     * @throws RuntimeException Iff any of the following is true:
     * <ul>
     *     <li>the {@code exceptionTransformer} completed abruptly;</li>
     *     <li>the most recent attempt is the {@linkplain #lastAttempt() last} one;</li>
     *     <li>retrying was broken via
     *     {@link #breakAndThrowIfRetryAnd(Supplier)} / {@link #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)} /
     *     {@link #markAsLastAttempt()};</li>
     *     <li>the {@code retryPredicate} completed abruptly;</li>
     *     <li>the {@code retryPredicate} is {@code false}.</li>
     * </ul>
     * The exception thrown represents the failed result of the associated retryable activity,
     * i.e., the caller must not do any more attempts.
     * @see #advanceOrThrow(Throwable, BiFunction, BiPredicate)
     */
    void advanceOrThrow(final RuntimeException attemptException, final BiFunction<Throwable, Throwable, Throwable> exceptionTransformer,
            final BiPredicate<RetryState, Throwable> retryPredicate) throws RuntimeException {
        try {
            doAdvanceOrThrow(attemptException, exceptionTransformer, retryPredicate, true);
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
     * @see #advanceOrThrow(RuntimeException, BiFunction, BiPredicate)
     */
    void advanceOrThrow(final Throwable attemptException, final BiFunction<Throwable, Throwable, Throwable> exceptionChooser,
            final BiPredicate<RetryState, Throwable> retryPredicate) throws Throwable {
        doAdvanceOrThrow(attemptException, exceptionChooser, retryPredicate, false);
    }

    /**
     * @param onlyRuntimeExceptions {@code true} iff the method must expect {@link #exception} and {@code attemptException} to be
     * {@link RuntimeException}s and must not explicitly handle other {@link Throwable} types, of which only {@link Error} is possible
     * as {@link RetryState} does not have any source of {@link Exception}s.
     */
    private void doAdvanceOrThrow(final Throwable attemptException,
            final BiFunction<Throwable, Throwable, Throwable> exceptionTransformer,
            final BiPredicate<RetryState, Throwable> retryPredicate,
            final boolean onlyRuntimeExceptions) throws Throwable {
        assertTrue(attempt() < attempts);
        assertNotNull(attemptException);
        if (onlyRuntimeExceptions) {
            assertTrue(isRuntime(attemptException));
        }
        assertTrue(!firstAttempt() || exception == null);
        Throwable newlyChosenException = transformException(exception, attemptException, onlyRuntimeExceptions, exceptionTransformer);
        if (lastAttempt()) {
            exception = newlyChosenException;
            throw exception;
        } else {
            // note that we must not update the state, e.g, `exception`, `loopState`, before calling `retryPredicate`
            boolean retry = decideRetry(this, attemptException, newlyChosenException, onlyRuntimeExceptions, retryPredicate);
            exception = newlyChosenException;
            if (retry) {
                assertTrue(loopState.advance());
            } else {
                throw exception;
            }
        }
    }

    /**
     * @param onlyRuntimeExceptions See {@link #doAdvanceOrThrow(Throwable, BiFunction, BiPredicate, boolean)}.
     */
    private static Throwable transformException(@Nullable final Throwable previouslyChosenException, final Throwable attemptException,
            final boolean onlyRuntimeExceptions, final BiFunction<Throwable, Throwable, Throwable> exceptionTransformer) {
        if (onlyRuntimeExceptions && previouslyChosenException != null) {
            assertTrue(isRuntime(previouslyChosenException));
        }
        Throwable result;
        try {
            result = assertNotNull(exceptionTransformer.apply(previouslyChosenException, attemptException));
            if (onlyRuntimeExceptions) {
                assertTrue(isRuntime(result));
            }
        } catch (Throwable exceptionChooserException) {
            if (onlyRuntimeExceptions && !isRuntime(exceptionChooserException)) {
                throw exceptionChooserException;
            }
            if (previouslyChosenException != null) {
                exceptionChooserException.addSuppressed(previouslyChosenException);
            }
            exceptionChooserException.addSuppressed(attemptException);
            throw exceptionChooserException;
        }
        return result;
    }

    /**
     * @param readOnlyRetryState Must not be mutated by this method.
     * @param onlyRuntimeExceptions See {@link #doAdvanceOrThrow(Throwable, BiFunction, BiPredicate, boolean)}.
     */
    private boolean decideRetry(final RetryState readOnlyRetryState, final Throwable attemptException, final Throwable newlyChosenException,
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
     * that breaking results in throwing an exception because the retry loop has more than one iteration iff the first iteration fails.
     * Does nothing and completes normally if called during the {@linkplain #firstAttempt() first attempt}.
     * This method is useful when the associated retryable activity detects that a retry attempt should not happen
     * despite having been started. Must not be called more than once per {@link RetryState}.
     * <p>
     * If the {@code predicate} completes abruptly, this method also completes abruptly with the same exception but does not break retrying;
     * if the {@code predicate} is {@code true}, then the method breaks retrying and completes abruptly by throwing the exception that is
     * currently deemed to be a prospective failed result of the associated retryable activity. The thrown exception must also be used
     * by the caller to complete the ongoing attempt.
     * <p>
     * If this method is called from
     * {@linkplain RetryingSyncSupplier#RetryingSyncSupplier(RetryState, BiFunction, BiPredicate, Supplier)
     * retry predicate / failed result chooser}, the behavior is unspecified.
     *
     * @param predicate {@code true} iff retrying needs to be broken.
     * The {@code predicate} is not called during the {@linkplain #firstAttempt() first attempt}.
     * @throws RuntimeException Iff any of the following is true:
     * <ul>
     *     <li>the {@code predicate} completed abruptly;</li>
     *     <li>this method broke retrying.</li>
     * </ul>
     * The exception thrown represents the failed result of the associated retryable activity.
     * @see #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)
     */
    public void breakAndThrowIfRetryAnd(final Supplier<Boolean> predicate) throws RuntimeException {
        assertFalse(loopState.lastIteration());
        if (!firstAttempt()) {
            assertNotNull(exception);
            assertTrue(exception instanceof RuntimeException);
            RuntimeException localException = (RuntimeException) exception;
            try {
                if (predicate.get()) {
                    loopState.markAsLastIteration();
                }
            } catch (RuntimeException predicateException) {
                predicateException.addSuppressed(localException);
                throw predicateException;
            }
            if (loopState.lastIteration()) {
                throw localException;
            }
        }
    }

    /**
     * This method is intended to be used by callback-based code. It is similar to {@link #breakAndThrowIfRetryAnd(Supplier)},
     * but instead of throwing an exception, it relays it to the {@code callback}.
     * <p>
     * If this method is called from
     * {@linkplain RetryingAsyncCallbackSupplier#RetryingAsyncCallbackSupplier(RetryState, BiFunction, BiPredicate, com.mongodb.internal.async.function.AsyncCallbackSupplier)
     * retry predicate / failed result chooser}, the behavior is unspecified.
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
     * methods, this method has effect even if called during the {@linkplain #firstAttempt() first attempt}.
     */
    public void markAsLastAttempt() {
        loopState.markAsLastIteration();
    }

    /**
     * Returns {@code true} iff the current attempt is the first one, i.e., no retries have been made.
     *
     * @see #attempts()
     */
    public boolean firstAttempt() {
        return loopState.firstIteration();
    }

    /**
     * Returns {@code true} iff the current attempt is known to be the last one, i.e., it is known that no more retries will be made.
     * An attempt is known to be the last one either because the number of {@linkplain #attempts() attempts} is limited and the current
     * attempt is the last one, or because {@link #breakAndThrowIfRetryAnd(Supplier)} /
     * {@link #breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)} / {@link #markAsLastAttempt()} was called.
     *
     * @see #attempts()
     */
    public boolean lastAttempt() {
        return attempt() == attempts - 1 || loopState.lastIteration();
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
     *     <li>0 if the number of retries is {@linkplain #RetryState() unlimited};</li>
     *     <li>1 if no retries are allowed;</li>
     *     <li>{@link #RetryState(int) retries} + 1 otherwise.</li>
     * </ul>
     *
     * @see #attempt()
     * @see #firstAttempt()
     * @see #lastAttempt()
     */
    public int attempts() {
        return attempts == INFINITE_ATTEMPTS ? 0 : attempts;
    }

    /**
     * Returns the exception that is currently deemed to be a prospective failed result of the associated retryable activity.
     * Note that this exception is not necessary the one from the most recent failed attempt.
     * Returns an {@linkplain Optional#isEmpty() empty} {@link Optional} iff called during the {@linkplain #firstAttempt() first attempt}.
     * <p>
     * In synchronous code the returned exception is of the type {@link RuntimeException}.
     */
    public Optional<Throwable> exception() {
        assertTrue(exception == null || !firstAttempt());
        return Optional.ofNullable(exception);
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
                + ", exception=" + exception
                + '}';
    }
}
