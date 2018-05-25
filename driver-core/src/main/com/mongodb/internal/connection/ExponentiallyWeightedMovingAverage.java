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

import com.mongodb.annotations.NotThreadSafe;

import static com.mongodb.assertions.Assertions.isTrueArgument;

@NotThreadSafe
class ExponentiallyWeightedMovingAverage {
    private final double alpha;
    private long average = -1;

    ExponentiallyWeightedMovingAverage(final double alpha) {
        isTrueArgument("alpha >= 0.0 and <= 1.0", alpha >= 0.0 && alpha <= 1.0);
        this.alpha = alpha;
    }

    void reset() {
        average = -1;
    }

    long addSample(final long sample) {
        if (average == -1) {
            average = sample;
        } else {
            average = (long) (alpha * sample + (1 - alpha) * average);
        }

        return average;
    }

    long getAverage() {
        return average == -1 ? 0 : average;
    }
}
