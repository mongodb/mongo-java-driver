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
 * A model describing a distinct operation.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/distinct/ Distinct
 */
public class DistinctModel<D> implements ExplainableModel {
    private final String fieldName;
    private D filter;
    private Long maxTimeMS;

    /**
     * Construct a new instance.
     *
     * @param fieldName the non-null field name to get the distinct values of
     */
    public DistinctModel(final String fieldName) {
        this.fieldName = notNull("fieldName", fieldName);
    }

    /**
     * Gets the field name to get the distinct values of
     * @return the field name, which may not be null
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Set the query filter to apply.
     *
     * @param filter a document describing the query filter, which may be null
     * @return this
     */
    public DistinctModel filter(final D filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     */
    public Object getFilter() {
        return filter;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is null, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public Long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time, which may be null
     * @param timeUnit the time unit, which may only be null if maxTime is
     * @return this
     */
    public DistinctModel maxTimeMS(final Long maxTime, final TimeUnit timeUnit) {
        if (maxTime == null) {
            maxTimeMS = null;
        } else {
            notNull("timeUnit", timeUnit);
            this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        }
        return this;
    }
}
