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
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MapReduceStatistics {

    private final int inputCount;
    private final int outputCount;
    private final int emitCount;
    private final int duration;

    public MapReduceStatistics(final int inputCount, final int outputCount, final int emitCount, final int duration) {
        this.inputCount = inputCount;
        this.outputCount = outputCount;
        this.emitCount = emitCount;
        this.duration = duration;
    }

    public int getInputCount() {
        return inputCount;
    }

    public int getOutputCount() {
        return outputCount;
    }

    public int getEmitCount() {
        return emitCount;
    }

    public int getDuration() {
        return duration;
    }
}
