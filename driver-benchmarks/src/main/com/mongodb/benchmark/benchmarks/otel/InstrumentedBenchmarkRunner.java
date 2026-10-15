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

package com.mongodb.benchmark.benchmarks.otel;

import com.mongodb.benchmark.framework.Benchmark;
import org.bson.Document;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link com.mongodb.benchmark.framework.BenchmarkRunner} variant that records, for every timed iteration,
 * the process CPU time and the time spent in garbage collection alongside the wall-clock time.
 * <p>
 * CPU time excludes time spent waiting on the server, so it exposes driver-side cost with far less noise
 * than throughput. Process CPU time includes the driver's background threads (server monitoring), which cost
 * the same in every tracing configuration. GC time varies between processes more than the cost of tracing
 * does, so CPU time excluding GC is the steadier measure of the driver's own work.
 */
final class InstrumentedBenchmarkRunner {
    private final Benchmark benchmark;
    private final int opsPerIteration;
    private final int numWarmupIterations;
    private final int numIterations;
    private final int minTimeSeconds;
    private final int maxTimeSeconds;

    private final com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    InstrumentedBenchmarkRunner(final Benchmark benchmark, final int opsPerIteration, final int numWarmupIterations,
            final int numIterations, final int minTimeSeconds, final int maxTimeSeconds) {
        this.benchmark = benchmark;
        this.opsPerIteration = opsPerIteration;
        this.numWarmupIterations = numWarmupIterations;
        this.numIterations = numIterations;
        this.minTimeSeconds = minTimeSeconds;
        this.maxTimeSeconds = maxTimeSeconds;
    }

    Document run() throws Exception {
        benchmark.setUp();
        for (int i = 0; i < numWarmupIterations; i++) {
            benchmark.before();
            benchmark.run();
            benchmark.after();
        }

        List<Long> wallNanos = new ArrayList<>();
        List<Long> cpuNanos = new ArrayList<>();
        List<Long> gcNanos = new ArrayList<>();
        long totalNanos = 0;
        for (int i = 0; shouldContinue(i, totalNanos); i++) {
            benchmark.before();
            long gcStart = gcTimeNanos();
            long cpuStart = os.getProcessCpuTime();
            long start = System.nanoTime();
            benchmark.run();
            long wall = System.nanoTime() - start;
            long cpu = os.getProcessCpuTime() - cpuStart;
            long gc = gcTimeNanos() - gcStart;
            benchmark.after();
            wallNanos.add(wall);
            cpuNanos.add(cpu);
            gcNanos.add(gc);
            totalNanos += wall;
        }
        benchmark.tearDown();

        List<Long> cpuExGc = new ArrayList<>(cpuNanos.size());
        for (int i = 0; i < cpuNanos.size(); i++) {
            cpuExGc.add(cpuNanos.get(i) - gcNanos.get(i));
        }
        double medianWallNanos = median(wallNanos);
        return new Document("test_name", benchmark.getName())
                .append("iterations", wallNanos.size())
                .append("ops_per_iteration", opsPerIteration)
                .append("bytes_per_iteration", benchmark.getBytesPerRun())
                .append("megabytes_per_sec", (benchmark.getBytesPerRun() / 1_000_000d) / (medianWallNanos / 1_000_000_000d))
                .append("wall_us_per_op", medianWallNanos / opsPerIteration / 1_000d)
                .append("cpu_us_per_op", median(cpuNanos) / opsPerIteration / 1_000d)
                .append("gc_us_per_op", median(gcNanos) / opsPerIteration / 1_000d)
                .append("cpu_ex_gc_us_per_op", median(cpuExGc) / opsPerIteration / 1_000d)
                .append("wall_nanos", wallNanos);
    }

    private long gcTimeNanos() {
        long millis = 0;
        for (GarbageCollectorMXBean bean : gcBeans) {
            long t = bean.getCollectionTime();
            if (t > 0) {
                millis += t;
            }
        }
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private boolean shouldContinue(final int iterationCount, final long totalNanos) {
        if (totalNanos < TimeUnit.SECONDS.toNanos(minTimeSeconds)) {
            return true;
        }
        if (totalNanos > TimeUnit.SECONDS.toNanos(maxTimeSeconds)) {
            return false;
        }
        return iterationCount < numIterations;
    }

    /** Median, averaging the middle two of an even-sized sample (unlike the spec's nearest-rank percentile). */
    static double median(final List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        if (n == 0) {
            return Double.NaN;
        }
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2d;
    }
}
