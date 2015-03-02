/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to an operation that atomically finds a document and replaces it.
 *
 * @mongodb.driver.manual reference/command/findAndModify/
 * @since 3.0
 */
public class FindOneAndReplaceOptions {
    private Bson projection;
    private Bson sort;
    private boolean upsert;
    private boolean returnOriginal = true;
    private long maxTimeMS;

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual tutorial/project-fields-from-query-results Projection
     */
    public Bson getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual tutorial/project-fields-from-query-results Projection
     */
    public FindOneAndReplaceOptions projection(final Bson projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public Bson getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public FindOneAndReplaceOptions sort(final Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public FindOneAndReplaceOptions upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * When true, return the original document from before it was replaced. The default is true.
     *
     * @return true if the original document should be returned
     */
    public boolean getReturnOriginal() {
        return returnOriginal;
    }

    /**
     * Set whether to return the original document from before it was replaced.
     *
     * @param returnOriginal set whether to return the original document from before it was replaced
     * @return this
     */
    public FindOneAndReplaceOptions returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public FindOneAndReplaceOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum execution time for the find one and replace operation.
     *
     * @param timeUnit the time unit for the result
     * @return the max time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }
}

