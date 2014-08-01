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


package com.mongodb.operation;


import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Options to determine how an aggregation pipeline is executed.
 *
 * @since 3.0
 */
public class AggregationOptions {
    public enum OutputMode {
        /**
         * The output of the aggregate operation is returned inline.
         */
        INLINE,
        /**
         * The output of the aggregate operation is returned using a cursor.
         */
        CURSOR
    }

    private final Integer batchSize;
    private final Boolean allowDiskUse;
    private final OutputMode outputMode;
    private final long maxTimeMS;

    public AggregationOptions(final Builder builder) {
        notNull("builder", builder);
        batchSize = builder.batchSize;
        allowDiskUse = builder.allowDiskUse;
        outputMode = builder.outputMode;
        maxTimeMS = builder.maxTimeMS;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the output mode, inline or cursor.
     *
     * @return the output mode
     */
    public OutputMode getOutputMode() {
        return outputMode;
    }

    /**
     * Gets the batch size to apply when a cursor is returned.
     *
     * @return the batch size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Gets whether disk use is allowed when executing the pipeline.
     *
     * @return is disk use allowed
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Gets the maximum execution time for the aggregation command.
     *
     * @param timeUnit the time unit for the result
     * @return the max time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    @Override
    public String toString() {
        return "AggregationOptions{" + "batchSize=" + batchSize + ", allowDiskUse=" + allowDiskUse + ", outputMode=" + outputMode + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregationOptions that = (AggregationOptions) o;

        if (maxTimeMS != that.maxTimeMS) {
            return false;
        }
        if (allowDiskUse != null ? !allowDiskUse.equals(that.allowDiskUse) : that.allowDiskUse != null) {
            return false;
        }
        if (batchSize != null ? !batchSize.equals(that.batchSize) : that.batchSize != null) {
            return false;
        }
        if (outputMode != that.outputMode) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = batchSize != null ? batchSize.hashCode() : 0;
        result = 31 * result + (allowDiskUse != null ? allowDiskUse.hashCode() : 0);
        result = 31 * result + outputMode.hashCode();
        result = 31 * result + (int) (maxTimeMS ^ (maxTimeMS >>> 32));
        return result;
    }

    public static class Builder {

        private Integer batchSize;
        private Boolean allowDiskUse;
        private OutputMode outputMode = OutputMode.INLINE;
        private long maxTimeMS;

        protected Builder() {
        }

        public Builder batchSize(final Integer size) {
            batchSize = size;
            return this;
        }

        public Builder allowDiskUse(final Boolean allow) {
            allowDiskUse = allow;
            return this;
        }

        public Builder outputMode(final OutputMode mode) {
            outputMode = mode;
            return this;
        }

        /**
         * Sets the maximum execution time for the aggregation command.
         *
         * @param maxTime the max time
         * @param timeUnit the time unit
         * @return this
         */
        public Builder maxTime(final long maxTime, final TimeUnit timeUnit) {
            maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
            return this;
        }

        public AggregationOptions build() {
            return new AggregationOptions(this);
        }
    }
}
