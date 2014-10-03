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

package com.mongodb.client.model;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing an aggregation.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/aggregation/ Aggregation
 * @mongodb.server.release 2.2
 */
public class AggregateOptions {
    private Boolean allowDiskUse;
    private Integer batchSize;
    private long maxTimeMS;
    private Boolean useCursor;

    /**
     * Whether writing to temporary files is enabled. A null value indicates that it's unspecified.
     *
     * @return true if writing to temporary files is enabled
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public AggregateOptions allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to null, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public AggregateOptions batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public AggregateOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets whether the server should use a cursor to return results.  The default value is null, in which case
     * a cursor will be used if the server supports it.
     *
     * @return whether the server should use a cursor to return results
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public Boolean getUseCursor() {
        return useCursor;
    }

    /**
     * Sets whether the server should use a cursor to return results.
     *
     * @param useCursor whether the server should use a cursor to return results
     * @return this
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public AggregateOptions useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }
}
