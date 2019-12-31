/*
 * Copyright 2016-present MongoDB, Inc.
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
 *
 */

package com.mongodb.benchmark.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BenchmarkRunner {
    private final Benchmark benchmark;
    private final int numWarmupIterations;
    private final int numIterations;
    private final int minTimeSeconds;
    private final int maxTimeSeconds;

    public BenchmarkRunner(final Benchmark benchmark, final int numWarmupIterations, int numIterations) {
        this(benchmark, numWarmupIterations, numIterations, 5, 30);
    }

    public BenchmarkRunner(final Benchmark benchmark, final int numWarmupIterations, int numIterations,
                           int minTimeSeconds, int maxTimeSeconds) {
        this.benchmark = benchmark;
        this.numWarmupIterations = numWarmupIterations;
        this.numIterations = numIterations;
        this.minTimeSeconds = minTimeSeconds;
        this.maxTimeSeconds = maxTimeSeconds;
    }

    public BenchmarkResult run() throws Exception {
        benchmark.setUp();

        for (int i = 0; i < numWarmupIterations; i++) {
            benchmark.before();

            benchmark.run();

            benchmark.after();
        }

        List<Long> elapsedTimeNanosList = new ArrayList<Long>(numIterations);

        long totalTimeNanos = 0;

        for (int i = 0; shouldContinue(i, totalTimeNanos); i++) {
            benchmark.before();

            long startTimeNanos = System.nanoTime();
            benchmark.run();
            long elapsedTimeNanos = System.nanoTime() - startTimeNanos;
            elapsedTimeNanosList.add(elapsedTimeNanos);
            totalTimeNanos += elapsedTimeNanos;

            benchmark.after();
        }

        benchmark.tearDown();

        return new BenchmarkResult(benchmark.getName(), elapsedTimeNanosList, benchmark.getBytesPerRun());
    }

    private boolean shouldContinue(final int iterationCount, final long totalTimeNanos) {
        if ((totalTimeNanos) < TimeUnit.SECONDS.toNanos(minTimeSeconds)) {
            return true;
        }

        if ((totalTimeNanos) > TimeUnit.SECONDS.toNanos(maxTimeSeconds)) {
            return false;
        }

        return iterationCount < numIterations;
    }

}
