/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.lang.Nullable;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to an operation that atomically finds a document and deletes it.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/findAndModify/
 */
public class FindOneAndDeleteOptions {
    private Bson projection;
    private Bson sort;
    private long maxTimeMS;
    private Collation collation;
    private Bson hint;
    private String hintString;
    private BsonValue comment;

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual tutorial/project-fields-from-query-results Projection
     */
    @Nullable
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
    public FindOneAndDeleteOptions projection(@Nullable final Bson projection) {
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
    @Nullable
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
    public FindOneAndDeleteOptions sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public FindOneAndDeleteOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum execution time for the find one and delete operation.
     *
     * @param timeUnit the time unit for the result
     * @return the max time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public FindOneAndDeleteOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint, which should describe an existing index
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a document describing the index which should be used for this operation.
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    public FindOneAndDeleteOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Sets the hint to apply.
     *
     * <p>Note: If {@link FindOneAndDeleteOptions#hint(Bson)} is set that will be used instead of any hint string.</p>
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.4
     */
    public FindOneAndDeleteOptions hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }


    /**
     * @return the comment for this operation. A null value means no comment is set.
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public FindOneAndDeleteOptions comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public FindOneAndDeleteOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String toString() {
        return "FindOneAndDeleteOptions{"
                + "projection=" + projection
                + ", sort=" + sort
                + ", maxTimeMS=" + maxTimeMS
                + ", collation=" + collation
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + ", comment=" + comment
                + '}';
    }
}
