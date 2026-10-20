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

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The part of {@link RetryControl} accessible to the methods of the {@link RetryPolicy} interface.
 * It prevents, for example, the method {@link RetryPolicy#onAttemptFailure(RetryContext, Throwable)} from calling
 * {@link RetryControl#breakAndThrowIfRetryAnd(Supplier)}, as that is forbidden.
 * A non-overriding method of a {@link RetryPolicy} implementation is free to access the full {@link RetryControl}
 * as opposed to {@link RetryContext}.
 */
@NotThreadSafe
public interface RetryContext {
    /**
     * Returns {@code true} iff the current attempt is the first one, i.e., no retry attempts have been made.
     *
     * @see #attempt()
     */
    boolean isFirstAttempt();

    /**
     * A 0-based attempt number.
     *
     * @see #isFirstAttempt()
     */
    int attempt();

    /**
     * Returns the exception that is currently deemed to be the prospective failed result of the retryable activity.
     * Note that it is not necessary the failed result of the most recent failed attempt.
     * Returns an {@linkplain Optional#isEmpty() empty} {@link Optional} iff called during the {@linkplain #isFirstAttempt() first attempt}.
     *
     * @see RetryPolicy.Decision#getProspectiveFailedResult()
     */
    Optional<Throwable> getProspectiveFailedResult();
}
