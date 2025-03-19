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
import java.util.Collections;
import java.util.List;

public class BenchmarkResult {
    private final String name;
    private final List<Long> elapsedTimeNanosList;
    private final List<Long> sortedElapsedTimeNanosList;
    private final int bytesPerRun;

    public BenchmarkResult(final String name, final List<Long> elapsedTimeNanosList, final int bytesPerRun) {
        this.name = name;
        this.elapsedTimeNanosList = new ArrayList<>(elapsedTimeNanosList);
        this.bytesPerRun = bytesPerRun;
        this.sortedElapsedTimeNanosList = new ArrayList<>(elapsedTimeNanosList);
        Collections.sort(this.sortedElapsedTimeNanosList);
    }

    public int getBytesPerIteration() {
        return bytesPerRun;
    }

    public String getName() {
        return name;
    }

    public List<Long> getElapsedTimeNanosList() {
        return elapsedTimeNanosList;
    }

    public long getElapsedTimeNanosAtPercentile(final int percentile) {
        return sortedElapsedTimeNanosList.get(Math.max(0, ((int) (getNumIterations() * percentile / 100.0)) - 1));
    }

    public int getNumIterations() {
        return elapsedTimeNanosList.size();
    }

    @Override
    public String toString() {
        return "BenchmarkResult{" +
                "name='" + name + '\'' +
                ", elapsedTimeNanosList=" + elapsedTimeNanosList +
                ", bytesPerRun=" + bytesPerRun +
                '}';
    }
}
