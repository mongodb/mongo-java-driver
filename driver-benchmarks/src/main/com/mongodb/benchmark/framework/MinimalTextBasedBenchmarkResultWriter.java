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

import static java.lang.String.format;

public class MinimalTextBasedBenchmarkResultWriter implements BenchmarkResultWriter {

    public static final double ONE_BILLION = 1000000000.0; // To convert nanoseconds to seconds
    private final PrintStream printStream;

    public MinimalTextBasedBenchmarkResultWriter(final PrintStream printStream) {
        this.printStream = printStream;
    }

    @Override
    public void write(final BenchmarkResult benchmarkResult) {
        printStream.println(format("%s: %.3f", benchmarkResult.getName(),
                benchmarkResult.getElapsedTimeNanosAtPercentile(50) / ONE_BILLION));
    }

    @Override
    public void close() {
    }
}
