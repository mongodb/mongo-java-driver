/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 * @since 2.12
 */
public class AggregationOptions {
    private final Integer batchSize;
    private final Boolean allowDiskUsage;
    private final OutputMode outputMode;
    private final long maxTimeMS;

    public enum OutputMode {
        /**
         * The output of the aggregate operation is returned inline.
         */
        INLINE {
            @Override
            public org.mongodb.AggregationOptions.OutputMode toNew() {
                return org.mongodb.AggregationOptions.OutputMode.INLINE;
            }
        },
        /**
         * The output of the aggregate operation is returned using a cursor.
         */
        CURSOR {
            @Override
            public org.mongodb.AggregationOptions.OutputMode toNew() {
                return org.mongodb.AggregationOptions.OutputMode.CURSOR;
            }
        };

        public abstract org.mongodb.AggregationOptions.OutputMode toNew();
    }

    AggregationOptions(final Builder builder) {
        batchSize = builder.batchSize;
        allowDiskUsage = builder.allowDiskUsage;
        outputMode = builder.outputMode;
        maxTimeMS = builder.maxTimeMS;
    }

    public Boolean getAllowDiskUsage() {
        return allowDiskUsage;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public OutputMode getOutputMode() {
        return outputMode;
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

    public org.mongodb.AggregationOptions toNew() {
        return org.mongodb.AggregationOptions.builder()
                          .batchSize(getBatchSize())
                          .allowDiskUsage(getAllowDiskUsage())
                          .outputMode(getOutputMode().toNew())
                          .maxTime(maxTimeMS, MILLISECONDS)
                          .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer batchSize;
        private Boolean allowDiskUsage;
        private OutputMode outputMode = OutputMode.INLINE;
        private long maxTimeMS;

        protected Builder() {
        }

        public Builder batchSize(final Integer size) {
            batchSize = size;
            return this;
        }

        public Builder allowDiskUsage(final Boolean allow) {
            allowDiskUsage = allow;
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
