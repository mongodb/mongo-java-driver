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
 * A model describing a count operation.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/count/ Count
 */
public class CountModel<D> implements ExplainableModel {
    private D filter;
    private D hint;
    private String hintString;
    private Long limit;
    private Long maxTimeMS;
    private Long skip;

    /**
     * Set the query filter to apply.
     *
     * @param filter a document describing the query filter, which may be null. The filter can be of any type for which a
     * {@code Codec} is registered
     * @return this
     */
    public CountModel<D> filter(final D filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     */
    public D getFilter() {
        return filter;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint
     */
    public D getHint() {
        return hint;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint
     */
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a document describing the index which should be used for this operation.  The hint can be of any type for which a
     * {@code Codec} is registered}
     * @return this
     */
    public CountModel<D> hint(final D hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return  this
     */
    public CountModel<D> hint(final String hint) {
        this.hintString = hint;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     */
    public Long getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     */
    public CountModel<D> limit(final Long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is null.
     *
     * @return the number of documents to skip, which may be null
     */
    public Long getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip, which may be null
     * @return this
     */
    public CountModel<D> skip(final Long skip) {
        this.skip = skip;
        return this;
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
    public CountModel<D> maxTimeMS(final Long maxTime, final TimeUnit timeUnit) {
        if (maxTime == null) {
            maxTimeMS = null;
        } else {
            notNull("timeUnit", timeUnit);
            this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        }
        return this;
    }

}
