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

import java.util.ArrayDeque;
import java.util.Deque;

final class RoundTripTimeSampler {
    private final ExponentiallyWeightedMovingAverage averageRoundTripTime = new ExponentiallyWeightedMovingAverage(0.2);
    private final RecentSamples recentSamples = new RecentSamples();

    void reset() {
        averageRoundTripTime.reset();
        recentSamples.reset();
    }

    void addSample(final long sample) {
        recentSamples.add(sample);
        averageRoundTripTime.addSample(sample);
    }

    long getAverage() {
        return averageRoundTripTime.getAverage();
    }

    long getMin() {
        return recentSamples.min();
    }

    private static final class RecentSamples {

        private static final int MAX_SIZE = 10;
        private final Deque<Long> samples;

        RecentSamples() {
            samples = new ArrayDeque<>();
        }

        void add(final long sample) {
            if (samples.size() == MAX_SIZE) {
                samples.removeFirst();
            }
            samples.add(sample);
        }

        void reset() {
            samples.clear();
        }

        long min() {
            // Clients MUST report the minimum RTT as 0 until at least 2 samples have been gathered
            return samples.size() < 2 ? 0 : samples.stream().min(Long::compareTo).orElse(0L);
        }
    }
}
