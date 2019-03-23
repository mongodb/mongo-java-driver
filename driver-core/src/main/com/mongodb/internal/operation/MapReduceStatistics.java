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

package com.mongodb.internal.operation;

/**
 * Common statistics returned by running all types of map-reduce operations.
 *
 * @since 3.0
 */
public class MapReduceStatistics {

    private final int inputCount;
    private final int outputCount;
    private final int emitCount;
    private final int duration;

    /**
     * Construct a new instance.
     *
     * @param inputCount  the input count.
     * @param outputCount the output count.
     * @param emitCount   the emit count.
     * @param duration    the duration.
     */
    public MapReduceStatistics(final int inputCount, final int outputCount, final int emitCount, final int duration) {
        this.inputCount = inputCount;
        this.outputCount = outputCount;
        this.emitCount = emitCount;
        this.duration = duration;
    }

    /**
     * Get the number of documents that were input into the map reduce operation
     *
     * @return the number of documents that read while processing this map reduce
     */
    public int getInputCount() {
        return inputCount;
    }

    /**
     * Get the number of documents generated as a result of this map reduce
     *
     * @return the number of documents output by the map reduce
     */
    public int getOutputCount() {
        return outputCount;
    }

    /**
     * Get the number of messages emitted from the provided map function.
     *
     * @return the number of items emitted from the map function
     */
    public int getEmitCount() {
        return emitCount;
    }

    /**
     * Get the amount of time it took to run the map-reduce.
     *
     * @return the duration in milliseconds
     */
    public int getDuration() {
        return duration;
    }
}
