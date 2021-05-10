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
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The options to apply to an operation that atomically finds a document and updates it.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/findAndModify/
 */
public class FindOneAndUpdateOptions {
    private Bson projection;
    private Bson sort;
    private boolean upsert;
    private ReturnDocument returnDocument = ReturnDocument.BEFORE;
    private long maxTimeMS;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private List<? extends Bson> arrayFilters;
    private Bson hint;
    private String hintString;

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
    public FindOneAndUpdateOptions projection(@Nullable final Bson projection) {
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
    public FindOneAndUpdateOptions sort(@Nullable final Bson sort) {
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
    public FindOneAndUpdateOptions upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Gets the {@link ReturnDocument} value indicating whether to return the document before it was updated / inserted or after
     *
     * @return {@link ReturnDocument#BEFORE} if returning the document before it was updated or inserted otherwise
     * returns {@link ReturnDocument#AFTER}
     */
    public ReturnDocument getReturnDocument() {
        return returnDocument;
    }

    /**
     * Set whether to return the document before it was updated / inserted or after
     *
     * @param returnDocument set whether to return the document before it was updated / inserted or after
     * @return this
     */
    public FindOneAndUpdateOptions returnDocument(final ReturnDocument returnDocument) {
        this.returnDocument = notNull("returnDocument", returnDocument);
        return this;
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @deprecated prefer {@code MongoCollection.withTimeout(long, TimeUnit)} instead
     */
    @Deprecated
    public FindOneAndUpdateOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum execution time for the find one and update operation.
     *
     * @param timeUnit the time unit for the result
     * @return the max time
     * @deprecated prefer {@code MongoCollection.withTimeout(long, TimeUnit)} instead
     */
    @Deprecated
    public long getMaxTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    @Nullable
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public FindOneAndUpdateOptions bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
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
    public FindOneAndUpdateOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }
    /**
     * Sets the array filters option
     *
     * @param arrayFilters the array filters, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public FindOneAndUpdateOptions arrayFilters(@Nullable final List<? extends Bson> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    /**
     * Returns the array filters option
     *
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    public List<? extends Bson> getArrayFilters() {
        return arrayFilters;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 4.1
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 4.1
     */
    public FindOneAndUpdateOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     */
    public FindOneAndUpdateOptions hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    @Override
    public String toString() {
        return "FindOneAndUpdateOptions{"
                + "projection=" + projection
                + ", sort=" + sort
                + ", upsert=" + upsert
                + ", returnDocument=" + returnDocument
                + ", maxTimeMS=" + maxTimeMS
                + ", bypassDocumentValidation=" + bypassDocumentValidation
                + ", collation=" + collation
                + ", arrayFilters=" + arrayFilters
                + ", hint=" + hint
                + ", hintString=" + hintString
                + '}';
    }
}
