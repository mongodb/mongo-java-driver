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
package com.mongodb.internal.time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @see TimePoint
 */
public interface StartTime {

    /**
     * @see TimePoint#elapsed()
     */
    Duration elapsed();

    /**
     * @see TimePoint#asTimeout()
     */
    Timeout asTimeout();

    /**
     * Returns an {@linkplain Timeout#isInfinite() infinite} timeout if
     * {@code timeoutValue} is negative, an expired timeout if
     * {@code timeoutValue} is 0, otherwise a timeout in {@code durationNanos}.
     * <p>
     * Note that some code might ignore a timeout, and attempt to perform
     * the operation in question at least once.</p>
     * <p>
     * Note that the contract of this method is also used in some places to
     * specify the behavior of methods that accept {@code (long timeout, TimeUnit unit)},
     * e.g., {@link com.mongodb.internal.connection.ConcurrentPool#get(long, TimeUnit)},
     * so it cannot be changed without updating those methods.</p>
     *
     * @see TimePoint#fromNowOrInfiniteIfNegative(long, TimeUnit)
     */
    Timeout fromNowOrInfiniteIfNegative(long timeoutValue, TimeUnit timeUnit);

    /**
     * @return a StartPoint, as of now
     */
    static StartTime now() {
        return TimePoint.at(System.nanoTime());
    }
}
