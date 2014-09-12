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

import com.mongodb.CursorFlag;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a find operation (also commonly referred to as a query).
 *
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/query-documents/ Find
 */
public final class FindModel<D> implements ExplainableModel {
    private D criteria;
    private Integer batchSize;
    private Integer limit;
    private D modifiers;
    private D projection;
    private EnumSet<CursorFlag> cursorFlags = EnumSet.noneOf(CursorFlag.class);
    private Long maxTimeMS;
    private Integer skip;
    private D sort;

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param criteria the criteria, which may be null. This can be of any type for which a {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindModel criteria(final D criteria) {
        this.criteria = criteria;
        return this;
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     */
    public D getCriteria() {
        return criteria;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     */
    public Integer getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     */
    public FindModel limit(final Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is null.
     *
     * @return the number of documents to skip, which may be null
     */
    public Integer getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip, which may be null
     * @return this
     */
    public FindModel skip(final Integer skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the cursor flags.
     *
     * @return the cursor flags
     */
    public EnumSet<CursorFlag> getCursorFlags() {
        return cursorFlags;
    }

    /**
     * Sets the cursor flags.
     *
     * @param cursorFlags the cursor flags
     * @return this
     */
    public FindModel cursorFlags(final EnumSet<CursorFlag> cursorFlags) {
        this.cursorFlags = cursorFlags;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is null, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public Long getMaxTime(final TimeUnit timeUnit) {
        if (maxTimeMS == null) {
            return null;
        }
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
    public FindModel maxTimeMS(final Long maxTime, final TimeUnit timeUnit) {
        if (maxTime == null) {
            maxTimeMS = null;
        } else {
            notNull("timeUnit", timeUnit);
            this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        }
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to null, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch
     * @param batchSize the batch size, which may be null
     * @return this
     */
    public FindModel batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public D getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null. This can be of any type for which a
     * {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public FindModel modifiers(final D modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public D getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null. This can be of any type for which a
     * {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/tutorial/project-fields-from-query-results Projection
     */
    public FindModel projection(final D projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public D getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null. This can be of any type for which a
     * {@code Codec} is registered
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindModel sort(final D sort) {
        this.sort = sort;
        return this;
    }
}
