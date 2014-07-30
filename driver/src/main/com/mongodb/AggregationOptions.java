/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;


import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to an aggregate operation.
 *
 * @mongodb.server.release 2.2
 * @since 2.12
 */
public class AggregationOptions {
    private final Integer batchSize;
    private final Boolean allowDiskUse;
    private final OutputMode outputMode;
    private final long maxTimeMS;

    public enum OutputMode {
        /**
         * The output of the aggregate operation is returned inline.
         */
        INLINE,

        /**
         * The output of the aggregate operation is returned using a cursor.
         *
         * @mongodb.server.release 2.6
         */
        CURSOR
    }

    AggregationOptions(final Builder builder) {
        batchSize = builder.batchSize;
        allowDiskUse = builder.allowDiskUse;
        outputMode = builder.outputMode;
        maxTimeMS = builder.maxTimeMS;
    }

    /**
     * If true, this enables external sort capabilities otherwise $sort produces an error if the operation consumes 10 percent or more of
     * RAM.
     *
     * @mongodb.server.release 2.6
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * The size of batches to use when iterating over results.
     *
     * @mongodb.server.release 2.6
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * The mode of output for this configuration.
     *
     * @see OutputMode
     */
    public OutputMode getOutputMode() {
        return outputMode;
    }

    /**
     * Gets the maximum execution time for the aggregation command.
     *
     * @param timeUnit the time unit for the result
     * @return the max time
     * @mongodb.server.release 2.6
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    com.mongodb.operation.AggregationOptions toNew() {
        return com.mongodb.operation.AggregationOptions.builder()
                          .batchSize(getBatchSize())
                          .allowDiskUse(getAllowDiskUse())
                          .outputMode(outputModeToNew())
                          .maxTime(maxTimeMS, MILLISECONDS)
                          .build();
    }

    private com.mongodb.operation.AggregationOptions.OutputMode outputModeToNew() {
        switch (getOutputMode()) {
            case INLINE:
                return com.mongodb.operation.AggregationOptions.OutputMode.INLINE;
            case CURSOR:
                return com.mongodb.operation.AggregationOptions.OutputMode.CURSOR;
            default:
                throw new IllegalArgumentException("Unsupported output mode");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AggregationOptions{");
        sb.append("allowDiskUse=")
            .append(allowDiskUse);
        sb.append(", batchSize=")
            .append(batchSize);
        sb.append(", outputMode=")
            .append(outputMode);
        sb.append(", maxTimeMS=")
            .append(maxTimeMS);
        sb.append('}');
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer batchSize;
        private Boolean allowDiskUse;
        private OutputMode outputMode = OutputMode.INLINE;
        private long maxTimeMS;

        private Builder() {
        }

        /**
         * Sets the size of batches to use when iterating over results.
         *
         * @return this
         * @mongodb.server.release 2.6
         */
        public Builder batchSize(final Integer size) {
            batchSize = size;
            return this;
        }

        public Builder allowDiskUse(final Boolean allowDiskUse) {
            this.allowDiskUse = allowDiskUse;
            return this;
        }

        /**
         * The mode of output for this configuration.
         *
         * @return this
         * @see OutputMode
         */
        public Builder outputMode(final OutputMode mode) {
            outputMode = mode;
            return this;
        }

        /**
         * Sets the maximum execution time for the aggregation command.
         *
         * @param maxTime  the max time
         * @param timeUnit the time unit
         * @return this
         * @mongodb.server.release 2.6
         */
        public Builder maxTime(final long maxTime, final TimeUnit timeUnit) {
            maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
            return this;
        }

        /**
         * Return the options based on this builder.
         *
         * @return the aggregation options
         */
        public AggregationOptions build() {
            return new AggregationOptions(this);
        }
    }
}
