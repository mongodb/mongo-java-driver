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

import com.mongodb.internal.async.function.RetryContext;
import com.mongodb.internal.async.function.RetryPolicy;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;

import java.util.function.Predicate;

final class SimpleRetryPolicy implements RetryPolicy {
    private final Predicate<Throwable> shouldRetry;

    SimpleRetryPolicy(final Predicate<Throwable> shouldRetry) {
        this.shouldRetry = shouldRetry;
    }

    @Override
    public Decision onAttemptFailure(final RetryContext retryContext, final Throwable attemptFailedResult) {
        return new Decision(attemptFailedResult, shouldRetry.test(attemptFailedResult) ? new RetryAttemptInfo() : null);
    }
}
