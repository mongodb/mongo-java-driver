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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

public class TextBasedBenchmarkResultWriter implements BenchmarkResultWriter {

    public static final double ONE_MEGABYTE = 1000000.0;   // Intentionally in base 10
    public static final double ONE_BILLION = 1000000000.0; // To convert nanoseconds to seconds
    private final PrintStream printStream;
    private final List<Integer> percentiles;
    private final boolean includeMegabytes;
    private final boolean includeRaw;

    public TextBasedBenchmarkResultWriter(final PrintStream printStream) {
        this(printStream, false, false);
    }

    public TextBasedBenchmarkResultWriter(final PrintStream printStream, boolean includeMegabytes, boolean includeRaw) {
        this(printStream, Arrays.asList(1, 10, 25, 50, 75, 90, 95, 99), includeMegabytes, includeRaw);
    }

    public TextBasedBenchmarkResultWriter(final PrintStream printStream, final List<Integer> percentiles) {
         this(printStream, percentiles, false, false);
    }

    public TextBasedBenchmarkResultWriter(final PrintStream printStream, final List<Integer> percentiles,
                                          boolean includeMegabytes, boolean includeRaw) {
        this.printStream = printStream;
        this.percentiles = percentiles;
        this.includeMegabytes = includeMegabytes;
        this.includeRaw = includeRaw;
    }

    @Override
    public void write(final BenchmarkResult benchmarkResult) {
        printStream.println(benchmarkResult.getName());
        printStream.println(benchmarkResult.getNumIterations() + " iterations");

        double megabytesPerIteration = benchmarkResult.getBytesPerIteration() / ONE_MEGABYTE;

        for (int percentile : percentiles) {
            double secondsPerIteration = benchmarkResult.getElapsedTimeNanosAtPercentile(percentile) / ONE_BILLION;
            printStream.println(format("%dth percentile: %.3f sec/iteration", percentile, secondsPerIteration));
        }

        if (includeMegabytes) {
            printStream.println();
            for (int percentile : percentiles) {
                double secondsPerIteration = benchmarkResult.getElapsedTimeNanosAtPercentile(percentile) / ONE_BILLION;
                printStream.println(format("%dth percentile: %.3f MB/sec", percentile, megabytesPerIteration / secondsPerIteration));
            }
        }

        if (includeRaw) {
            printStream.println();
            for (int i = 0; i < benchmarkResult.getElapsedTimeNanosList().size(); i++) {
                double secondsPerIteration = benchmarkResult.getElapsedTimeNanosList().get(i) / ONE_BILLION;
                printStream.println(format("%d: %.3f sec/iteration", i, secondsPerIteration));
            }
        }

        printStream.println();
        printStream.println();
    }

    @Override
    public void close() {
    }
}
