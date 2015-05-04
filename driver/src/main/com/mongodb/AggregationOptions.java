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


import com.mongodb.annotations.NotThreadSafe;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to an aggregate operation.
 *
 * @mongodb.server.release 2.2
 * @mongodb.driver.manual reference/command/aggregate/ aggregate
 * @since 2.12
 */
public class AggregationOptions {
    private final Integer batchSize;
    private final Boolean allowDiskUse;
    private final OutputMode outputMode;
    private final long maxTimeMS;

    /**
     * Enumeration to define where the results of the aggregation will be output.
     */
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
     * If true, this enables external sort capabilities, otherwise $sort produces an error if the operation consumes 10 percent or more of
     * RAM.
     *
     * @return true if aggregation stages can write data to temporary files
     * @mongodb.server.release 2.6
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * The size of batches to use when iterating over results.
     *
     * @return the batch size
     * @mongodb.server.release 2.6
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * The mode of output for this configuration.
     *
     * @return whether the output will be inline or via a cursor
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
     * @since 2.12
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AggregationOptions{");
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

    /**
     * Creates a new Builder for {@code AggregationOptions}.
     *
     * @return a new empty builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@code AggregationOptions}.
     *
     * @mongodb.server.release 2.2
     * @mongodb.driver.manual reference/command/aggregate/ aggregate
     */
    @NotThreadSafe
    public static class Builder {
        private Integer batchSize;
        private Boolean allowDiskUse;
        private OutputMode outputMode = OutputMode.INLINE;
        private long maxTimeMS;

        private Builder() {
        }

        /**
         * Sets the size of batches to use when iterating over results. Can be null.
         *
         * @param size the batch size to apply to the cursor
         * @return {@code this} so calls can be chained
         * @mongodb.server.release 2.6
         */
        public Builder batchSize(final Integer size) {
            batchSize = size;
            return this;
        }

        /**
         * Set whether to enable external sort capabilities. If set to false, $sort produces an error if the operation consumes 10 percent
         * or more RAM.
         *
         * @param allowDiskUse whether or not aggregation stages can write data to temporary files
         * @return {@code this} so calls can be chained
         * @mongodb.server.release 2.6
         */
        public Builder allowDiskUse(final Boolean allowDiskUse) {
            this.allowDiskUse = allowDiskUse;
            return this;
        }

        /**
         * The mode of output for this configuration.
         *
         * @param mode an {@code OutputMode} that defines how to output the results of the aggregation
         * @return {@code this} so calls can be chained
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
         * @return {@code this} so calls can be chained
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
