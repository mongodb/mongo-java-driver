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
import com.mongodb.lang.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * Customizes retrying and may allow for control beyond what {@link RetryControl} itself provides, depending on the implementation.
 * <p>
 * An implementation may be stateful and does not have to be thread-safe.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 */
@NotThreadSafe
public interface RetryPolicy {
    /**
     * This method is called exactly once per failed attempt,
     * even if that is the {@linkplain RetryControl#breakAndThrowIfRetryAnd(Supplier) last attempt},
     * provided that retrying is not {@linkplain RetryControl#doWhileDisabled(Supplier) disabled}.
     * If this method completes abruptly, then another attempt is not executed,
     * and the exception thrown by the method is used as the failed result of the retryable activity.
     * <p>
     * This method may have side effects, and may mutate {@link RetryContext#getProspectiveFailedResult()}, {@code attemptFailedResult}.
     *
     * @param attemptFailedResult The failed result of the most recent attempt.
     */
    Decision onAttemptFailure(RetryContext retryContext, Throwable attemptFailedResult);

    final class Decision {
        private final Throwable prospectiveFailedResult;
        @Nullable
        private final RetryAttemptInfo immediateNextAttemptInfo;

        /**
         * @param immediateNextAttemptInfo See {@link #getImmediateNextAttemptInfo()}.
         */
        public Decision(final Throwable prospectiveFailedResult, @Nullable final RetryAttemptInfo immediateNextAttemptInfo) {
            assertNotNull(prospectiveFailedResult);
            this.prospectiveFailedResult = prospectiveFailedResult;
            this.immediateNextAttemptInfo = immediateNextAttemptInfo;
        }

        /**
         * @see RetryControl#getProspectiveFailedResult()
         */
        public Throwable getProspectiveFailedResult() {
            return prospectiveFailedResult;
        }

        /**
         * Returns {@link Optional#isEmpty()} to signal that another attempt must not be executed.
         * If {@link RetryAttemptInfo} is {@linkplain Optional#isPresent() present},
         * another attempt is still not executed if most recent attempt was the {@linkplain RetryControl#breakAndThrowIfRetryAnd(Supplier) last one}.
         */
        public Optional<RetryAttemptInfo> getImmediateNextAttemptInfo() {
            return Optional.ofNullable(immediateNextAttemptInfo);
        }

        @Override
        public String toString() {
            return "Decision{"
                    + "prospectiveFailedResult=" + prospectiveFailedResult
                    + ", immediateNextAttemptInfo=" + immediateNextAttemptInfo
                    + '}';
        }

        /**
         * The information needed to start a retry attempt.
         */
        public static final class RetryAttemptInfo {
            public RetryAttemptInfo() {
            }

            @Override
            public String toString() {
                return "RetryAttemptInfo{}";
            }
        }
    }
}
