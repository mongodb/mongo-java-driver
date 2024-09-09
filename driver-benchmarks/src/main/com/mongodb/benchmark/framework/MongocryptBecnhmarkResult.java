package com.mongodb.benchmark.framework;
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

public class MongocryptBecnhmarkResult {
    private final String testName;
    private final int threadCount;
    private final long medianOpsPerSec;
    private final String createdAt;
    private final String completedAt;
    private final String metricName;
    private final String metricType;

    public MongocryptBecnhmarkResult(final String testName,
                                     final int threadCount,
                                     final long medianOpsPerSec,
                                     final String createdAt,
                                     final String completedAt,
                                     final String metricName,
                                     final String metricType) {
        this.testName = testName;
        this.threadCount = threadCount;
        this.medianOpsPerSec = medianOpsPerSec;
        this.createdAt = createdAt;
        this.completedAt = completedAt;
        this.metricName = metricName;
        this.metricType = metricType;
    }

    public String getTestName() {
        return testName;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public long getMedianOpsPerSec() {
        return medianOpsPerSec;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getCompletedAt() {
        return completedAt;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getMetricType() {
        return metricType;
    }

    @Override
    public String toString() {
        return "MongocryptBecnhmarkResult{" +
                "testName='" + testName + '\'' +
                ", threadCount=" + threadCount +
                ", medianOpsPerSec=" + medianOpsPerSec +
                ", createdAt=" + createdAt +
                ", completedAt=" + completedAt +
                ", metricName=" + metricName +
                ", metricType=" + metricType +
                '}';
    }
}
