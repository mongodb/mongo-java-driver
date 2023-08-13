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

package com.mongodb.internal.connection;

import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.assertions.Assertions.isTrueArgument;

class ExponentiallyWeightedMovingAverage {
    private static final long EMPTY = -1;

    private final double alpha;
    private final AtomicLong average;

    ExponentiallyWeightedMovingAverage(final double alpha) {
        isTrueArgument("alpha >= 0.0 and <= 1.0", alpha >= 0.0 && alpha <= 1.0);
        this.alpha = alpha;
        average = new AtomicLong(EMPTY);
    }

    void reset() {
        average.set(EMPTY);
    }

    long addSample(final long sample) {
        return average.accumulateAndGet(sample, (average, givenSample) -> {
            if (average == EMPTY) {
                return givenSample;
            }
            return (long) (alpha * givenSample + (1 - alpha) * average);
        });
    }

    long getAverage() {
        long average = this.average.get();
        return average == EMPTY ? 0 : average;
    }
}
