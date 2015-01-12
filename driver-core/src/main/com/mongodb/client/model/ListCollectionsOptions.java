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
 * The options to apply to a list collections operation.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/listCollections listCollections
 */
public final class ListCollectionsOptions {
    private Object filter;
    private int batchSize;
    private long maxTimeMS;

    /**
     * Construct a new instance.
     */
    public ListCollectionsOptions() {
    }

    /**
     * Gets the filter to apply to the query.
     *
     * @return the filter to apply to the query.
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public Object getFilter() {
        return this.filter;
    }

    /**
     * Sets the filter to apply to the query.
     *
     * @param filter the filter.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public ListCollectionsOptions filter(final Object filter) {
        notNull("filter", filter);
        this.filter = filter;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
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
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public ListCollectionsOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ListCollectionsOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public String toString() {
        return "ListCollectionOptions{"
                + ", filter=" + filter
                + ", batchSize=" + batchSize
                + ", maxTimeMS=" + maxTimeMS
                + '}';
    }
}
